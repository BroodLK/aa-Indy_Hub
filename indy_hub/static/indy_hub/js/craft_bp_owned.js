/**
 * Craft Blueprint "Owned Materials" allocation engine.
 *
 * Single source of truth for what the user already has in stock. Every tab
 * (Plan tree, Purchase Planner, Shopping List, Buy BPCs) reads its demand from
 * here so owned quantities are applied exactly once, in a deterministic order.
 *
 * The allocation runs in two passes, mirroring how a builder actually works:
 *
 *   Pass 1 - components. Walk the production tree top-down. Owned quantities
 *            are consumed against craftable nodes first. Covering a component
 *            removes its entire sub-tree of material demand, so the raw inputs
 *            it would have needed never reach the shopping list at all.
 *   Pass 2 - raw materials. Only once the whole tree has been walked, whatever
 *            is left in the owned pools is applied to the remaining leaf demand.
 *
 * Loaded before craft_bp_simulation_api.js so the simulation state can build
 * its demand maps on top of this.
 */
(function () {
    'use strict';

    // ==================== payload helpers ====================

    function getPayload() {
        return window.BLUEPRINT_DATA || {};
    }

    function getRootNodes() {
        const payload = getPayload();
        return Array.isArray(payload.materials_tree) ? payload.materials_tree : [];
    }

    function readNodeChildren(node) {
        if (Array.isArray(node && node.sub_materials)) {
            return node.sub_materials;
        }
        if (Array.isArray(node && node.subMaterials)) {
            return node.subMaterials;
        }
        return [];
    }

    function readNodeTypeId(node) {
        return Number(node && (node.type_id || node.typeId)) || 0;
    }

    function readNodeTypeName(node) {
        return String((node && (node.type_name || node.typeName)) || '');
    }

    function readNodeQuantity(node) {
        const raw = Number(node && (node.quantity ?? node.qty ?? 0));
        return Number.isFinite(raw) ? raw : 0;
    }

    /**
     * Ceil, but tolerant of binary-float drift.
     *
     * Nested demand is scaled by chained ratios, and those land microscopically
     * above an exact integer: 100 * (11 / 20) === 55.00000000000001. A plain
     * Math.ceil would turn that into 56 and inflate every downstream material by
     * one unit per affected node.
     */
    function ceilQuantity(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric) || numeric <= 0) {
            return 0;
        }
        const rounded = Math.round(numeric);
        const tolerance = Math.max(1e-6, Math.abs(numeric) * 1e-9);
        if (Math.abs(numeric - rounded) <= tolerance) {
            return rounded;
        }
        return Math.ceil(numeric);
    }

    /**
     * craft_bp.js declares CRAFT_MANUAL_FINANCIAL_STATE as a top-level const, which
     * lives in shared script scope rather than on window. Reading it before that
     * script has run throws a TDZ ReferenceError, hence the try/catch.
     */
    function getExcludedFinancialTypeIds() {
        try {
            if (typeof CRAFT_MANUAL_FINANCIAL_STATE !== 'undefined'
                && CRAFT_MANUAL_FINANCIAL_STATE
                && CRAFT_MANUAL_FINANCIAL_STATE.excludedTypeIds instanceof Set) {
                return CRAFT_MANUAL_FINANCIAL_STATE.excludedTypeIds;
            }
        } catch (error) {
            // craft_bp.js has not been evaluated yet.
        }
        return new Set();
    }

    /**
     * Manual planner rows (compressed ore swapped in for minerals) both feed the
     * name lookup and change which raw types pass 2 may consume, so they belong
     * in the cache key.
     */
    function getManualFinancialItems() {
        if (typeof window.getManualFinancialStateItems !== 'function') {
            return [];
        }
        try {
            const items = window.getManualFinancialStateItems();
            return Array.isArray(items) ? items : [];
        } catch (error) {
            return [];
        }
    }

    // ==================== owned input parsing ====================

    function normalizeOwnedMaterialName(value) {
        return String(value || '')
            .toLowerCase()
            .replace(/[\u2010-\u2015]/g, '-')
            .replace(/\s*-\s*/g, '-')
            .replace(/\s+/g, ' ')
            .trim();
    }

    function parseOwnedMaterialsInput(rawText) {
        const byName = new Map();
        const labelsByName = new Map();
        const malformedLines = [];
        const lines = String(rawText || '').split(/\r?\n/);

        lines.forEach((rawLine, index) => {
            const line = String(rawLine || '').trim();
            if (!line) {
                return;
            }

            let namePart = '';
            let qtyPart = '';

            const tabParts = line
                .split('\t')
                .map((part) => part.trim())
                .filter((part) => part !== '');
            if (tabParts.length >= 2) {
                qtyPart = tabParts[tabParts.length - 1];
                namePart = tabParts.slice(0, -1).join(' ');
            } else {
                const match = line.match(/^(.*\S)\s+([+-]?\d[\d,._]*)$/);
                if (match) {
                    namePart = String(match[1] || '').trim();
                    qtyPart = String(match[2] || '').trim();
                }
            }

            const normalizedName = normalizeOwnedMaterialName(namePart);
            const numericQty = Number(String(qtyPart || '').replace(/[,_\s]/g, ''));
            const qty = Number.isFinite(numericQty) ? Math.floor(numericQty) : 0;

            if (!normalizedName || qty <= 0) {
                malformedLines.push(index + 1);
                return;
            }

            byName.set(normalizedName, (byName.get(normalizedName) || 0) + qty);
            if (!labelsByName.has(normalizedName)) {
                labelsByName.set(normalizedName, namePart);
            }
        });

        return {
            byName,
            labelsByName,
            malformedLines,
        };
    }

    function buildOwnedTypeLookupFromPayload(ownedByName, labelsByName) {
        const payload = getPayload();
        const nameToTypeIds = new Map();
        const byType = new Map();
        const matchedNames = new Set();
        const unresolvedNames = [];
        const chosenTypeIdByName = new Map();
        const typeLabelsById = new Map();

        const addNameType = (typeId, typeName) => {
            const numericTypeId = Number(typeId) || 0;
            const resolvedTypeName = String(typeName || '').trim();
            const normalizedName = normalizeOwnedMaterialName(resolvedTypeName);
            if (!numericTypeId || !normalizedName) {
                return;
            }
            if (!nameToTypeIds.has(normalizedName)) {
                nameToTypeIds.set(normalizedName, new Set());
            }
            nameToTypeIds.get(normalizedName).add(numericTypeId);
            if (!typeLabelsById.has(numericTypeId) && resolvedTypeName) {
                typeLabelsById.set(numericTypeId, resolvedTypeName);
            }
        };

        const walkTree = (nodes) => {
            (Array.isArray(nodes) ? nodes : []).forEach((node) => {
                addNameType(readNodeTypeId(node), readNodeTypeName(node));
                const children = readNodeChildren(node);
                if (children.length > 0) {
                    walkTree(children);
                }
            });
        };

        const addArrayEntries = (items) => {
            (Array.isArray(items) ? items : []).forEach((item) => {
                const typeId = Number(item && (item.type_id || item.typeId)) || 0;
                const typeName = String((item && (item.type_name || item.typeName)) || '');
                addNameType(typeId, typeName);
            });
        };

        walkTree(payload.materials_tree);
        addArrayEntries(payload.materials);
        addArrayEntries(payload.direct_materials);
        addArrayEntries(getManualFinancialItems());
        if (typeof window.getPurchasePlannerItemsFromDom === 'function') {
            addArrayEntries(window.getPurchasePlannerItemsFromDom());
        }

        const grouped = payload.materials_by_group || payload.materialsByGroup || {};
        Object.values(grouped).forEach((group) => {
            if (!group || !Array.isArray(group.items)) {
                return;
            }
            addArrayEntries(group.items);
        });

        (ownedByName instanceof Map ? ownedByName : new Map()).forEach((qty, normalizedName) => {
            const typeIds = nameToTypeIds.get(normalizedName);
            if (!typeIds || typeIds.size === 0) {
                unresolvedNames.push(
                    (labelsByName instanceof Map ? labelsByName.get(normalizedName) : '') || normalizedName
                );
                return;
            }

            const chosenTypeId = Array.from(typeIds)[0];
            byType.set(chosenTypeId, (byType.get(chosenTypeId) || 0) + qty);
            matchedNames.add(normalizedName);
            chosenTypeIdByName.set(normalizedName, chosenTypeId);
        });

        return {
            byType,
            matchedNames,
            unresolvedNames,
            chosenTypeIdByName,
            typeLabelsById,
        };
    }

    // ==================== owned pools ====================

    /**
     * Two pools are kept: one keyed by resolved type id, one keyed by normalized
     * name for entries that could not be resolved to a type id at all. Names that
     * DID resolve are deliberately excluded from the name pool - seeding both would
     * let the same stock be spent twice.
     */
    function buildOwnedPools(ownedData, ownedLookup) {
        const byType = new Map(ownedLookup.byType || []);
        const byName = new Map();
        const matchedNames = ownedLookup.matchedNames instanceof Set ? ownedLookup.matchedNames : new Set();

        (ownedData.byName instanceof Map ? ownedData.byName : new Map()).forEach((qty, normalizedName) => {
            if (!normalizedName || matchedNames.has(normalizedName)) {
                return;
            }
            byName.set(normalizedName, Math.max(0, Number(qty) || 0));
        });

        return { byType, byName };
    }

    function drainOwnedPools(pools, typeId, typeName, wantedQty) {
        let remaining = ceilQuantity(wantedQty);
        if (remaining <= 0) {
            return 0;
        }

        let used = 0;
        const numericTypeId = Number(typeId) || 0;

        if (numericTypeId) {
            const available = Math.max(0, Number(pools.byType.get(numericTypeId) || 0));
            const take = Math.min(remaining, available);
            if (take > 0) {
                pools.byType.set(numericTypeId, available - take);
                used += take;
                remaining -= take;
            }
        }

        if (remaining > 0) {
            const normalizedName = normalizeOwnedMaterialName(typeName);
            if (normalizedName) {
                const available = Math.max(0, Number(pools.byName.get(normalizedName) || 0));
                const take = Math.min(remaining, available);
                if (take > 0) {
                    pools.byName.set(normalizedName, available - take);
                    used += take;
                }
            }
        }

        return used;
    }

    // ==================== switch state ====================

    function resolveSwitchState(typeId, craftable) {
        if (!craftable) {
            return 'prod';
        }

        const api = window.SimulationAPI;
        if (api && typeof api.getSwitchState === 'function') {
            const state = api.getSwitchState(typeId);
            if (state === 'buy' || state === 'prod' || state === 'useless') {
                return state;
            }
        }

        const switchEl = document.querySelector(`#tab-tree input.mat-switch[data-type-id="${typeId}"]`);
        if (switchEl) {
            if (switchEl.dataset.fixedMode === 'useless' || switchEl.dataset.userState === 'useless') {
                return 'useless';
            }
            return switchEl.checked ? 'prod' : 'buy';
        }

        return 'prod';
    }

    // ==================== cache signature ====================

    const treeIdentityTokens = new WeakMap();
    let nextTreeIdentityToken = 1;

    function getTreeIdentityToken(rootNodes) {
        if (!Array.isArray(rootNodes)) {
            return 'none';
        }
        if (!treeIdentityTokens.has(rootNodes)) {
            treeIdentityTokens.set(rootNodes, nextTreeIdentityToken);
            nextTreeIdentityToken += 1;
        }
        return String(treeIdentityTokens.get(rootNodes));
    }

    function collectCraftableTypeIds(rootNodes) {
        const ids = new Set();
        const walk = (nodes) => {
            (Array.isArray(nodes) ? nodes : []).forEach((node) => {
                const children = readNodeChildren(node);
                if (children.length === 0) {
                    return;
                }
                const typeId = readNodeTypeId(node);
                if (typeId) {
                    ids.add(typeId);
                }
                walk(children);
            });
        };
        walk(rootNodes);
        return Array.from(ids).sort((left, right) => left - right);
    }

    function getOwnedMaterialsRawText() {
        const inputEl = document.getElementById('ownedMaterialsInput');
        return inputEl ? String(inputEl.value || '') : '';
    }

    /**
     * The pasted list can be tens of thousands of characters and the signature is
     * rebuilt on every cache probe, so digest it instead of embedding it.
     */
    function hashText(text) {
        const value = String(text || '');
        let hash = 5381;
        for (let index = 0; index < value.length; index += 1) {
            hash = (((hash << 5) + hash) ^ value.charCodeAt(index)) >>> 0;
        }
        return `${value.length}.${hash.toString(36)}`;
    }

    function computeAllocationSignature(rootNodes) {
        const runsEl = document.getElementById('runsInput');
        const runs = runsEl
            ? String(runsEl.value || '')
            : String(getPayload().num_runs ?? '');
        const stateTokens = collectCraftableTypeIds(rootNodes)
            .map((typeId) => `${typeId}:${resolveSwitchState(typeId, true)}`);

        // Pass 2 skips excluded types and the manual rows feed the name lookup,
        // so a mineral -> compressed ore swap has to bust this cache.
        const excludedToken = Array.from(getExcludedFinancialTypeIds())
            .map((typeId) => Number(typeId) || 0)
            .sort((left, right) => left - right)
            .join(',');
        const manualToken = getManualFinancialItems()
            .map((item) => `${Number(item?.typeId ?? item?.type_id) || 0}:${Math.max(0, Number(item?.quantity ?? item?.qty) || 0)}`)
            .sort()
            .join(',');

        return [
            `tree=${getTreeIdentityToken(rootNodes)}`,
            `runs=${runs}`,
            `modes=${stateTokens.join(',')}`,
            `excluded=${excludedToken}`,
            `manual=${hashText(manualToken)}`,
            `owned=${hashText(getOwnedMaterialsRawText())}`,
        ].join(' | ');
    }

    // ==================== allocation ====================

    function addToCounter(map, typeId, qty) {
        const numericTypeId = Number(typeId) || 0;
        const amount = ceilQuantity(qty);
        if (!numericTypeId || amount <= 0) {
            return;
        }
        map.set(numericTypeId, (map.get(numericTypeId) || 0) + amount);
    }

    function buildAllocation() {
        const rootNodes = getRootNodes();
        const ownedData = parseOwnedMaterialsInput(getOwnedMaterialsRawText());
        const ownedLookup = buildOwnedTypeLookupFromPayload(ownedData.byName, ownedData.labelsByName);
        const pools = buildOwnedPools(ownedData, ownedLookup);

        const grossLeafNeeds = new Map();
        const grossProdCraftables = new Map();
        const grossBuyCraftables = new Map();
        const netProdCraftables = new Map();
        const netBuyCraftables = new Map();
        const netLeafNeeds = new Map();
        const componentAppliedByType = new Map();
        const rawAppliedByType = new Map();
        const appliedByType = new Map();
        const requiredByType = new Map();
        const typeNamesById = new Map();

        const rememberTypeName = (typeId, typeName) => {
            const numericTypeId = Number(typeId) || 0;
            const resolved = String(typeName || '').trim();
            if (numericTypeId && resolved && !typeNamesById.has(numericTypeId)) {
                typeNamesById.set(numericTypeId, resolved);
            }
        };

        // ---------- Pass 1: components ----------
        // Owned stock is spent on craftable nodes first, top-down. A covered
        // component takes its whole sub-tree of demand with it.
        const walkComponents = (nodes, multiplier) => {
            (Array.isArray(nodes) ? nodes : []).forEach((node) => {
                const typeId = readNodeTypeId(node);
                if (!typeId) {
                    return;
                }

                const rawQty = readNodeQuantity(node);
                if (rawQty <= 0) {
                    return;
                }

                const requiredQty = ceilQuantity(rawQty * multiplier);
                if (requiredQty <= 0) {
                    return;
                }

                const typeName = readNodeTypeName(node);
                rememberTypeName(typeId, typeName);

                const children = readNodeChildren(node);
                const craftable = children.length > 0;
                const state = resolveSwitchState(typeId, craftable);
                if (state === 'useless') {
                    return;
                }

                addToCounter(requiredByType, typeId, requiredQty);

                if (!craftable) {
                    // Leaves are deliberately left untouched until pass 2.
                    addToCounter(grossLeafNeeds, typeId, requiredQty);
                    return;
                }

                addToCounter(state === 'buy' ? grossBuyCraftables : grossProdCraftables, typeId, requiredQty);

                const usedQty = drainOwnedPools(pools, typeId, typeName, requiredQty);
                if (usedQty > 0) {
                    addToCounter(componentAppliedByType, typeId, usedQty);
                }

                const netQty = Math.max(0, requiredQty - usedQty);
                addToCounter(state === 'buy' ? netBuyCraftables : netProdCraftables, typeId, netQty);

                if (state === 'buy' || netQty <= 0) {
                    return;
                }

                walkComponents(children, multiplier * (netQty / requiredQty));
            });
        };

        walkComponents(rootNodes, 1);

        // ---------- Pass 2: raw materials ----------
        // Whatever survived pass 1 is now applied to the leaf demand that is
        // actually still needed.
        const excludedTypeIds = getExcludedFinancialTypeIds();
        grossLeafNeeds.forEach((requiredQty, typeId) => {
            if (excludedTypeIds.has(typeId)) {
                // Swapped out of the planner (e.g. minerals replaced by compressed
                // ore). Leave the stock in the pool so it shows up as leftover.
                netLeafNeeds.set(typeId, requiredQty);
                return;
            }

            const usedQty = drainOwnedPools(pools, typeId, typeNamesById.get(typeId) || '', requiredQty);
            if (usedQty > 0) {
                addToCounter(rawAppliedByType, typeId, usedQty);
            }
            netLeafNeeds.set(typeId, Math.max(0, requiredQty - usedQty));
        });

        componentAppliedByType.forEach((qty, typeId) => addToCounter(appliedByType, typeId, qty));
        rawAppliedByType.forEach((qty, typeId) => addToCounter(appliedByType, typeId, qty));

        return {
            ownedData,
            ownedLookup,
            grossLeafNeeds,
            grossProdCraftables,
            grossBuyCraftables,
            netLeafNeeds,
            netProdCraftables,
            netBuyCraftables,
            componentAppliedByType,
            rawAppliedByType,
            appliedByType,
            requiredByType,
            typeNamesById,
            remainingOwnedByType: pools.byType,
            remainingOwnedByName: pools.byName,
            hasOwnedEntries: ownedData.byName.size > 0,
        };
    }

    let cachedAllocation = null;
    let cachedSignature = null;

    function invalidateOwnedAllocation() {
        cachedAllocation = null;
        cachedSignature = null;
    }

    function computeOwnedAllocation(options = {}) {
        const rootNodes = getRootNodes();
        const signature = computeAllocationSignature(rootNodes);
        if (!options.force && cachedAllocation && cachedSignature === signature) {
            return cachedAllocation;
        }

        const allocation = buildAllocation();
        allocation.signature = signature;
        cachedAllocation = allocation;
        cachedSignature = signature;
        return allocation;
    }

    function getOwnedAppliedQty(typeId) {
        const allocation = computeOwnedAllocation();
        return Math.max(0, Number(allocation.appliedByType.get(Number(typeId) || 0) || 0));
    }

    function getRequiredQty(typeId) {
        const allocation = computeOwnedAllocation();
        return Math.max(0, Number(allocation.requiredByType.get(Number(typeId) || 0) || 0));
    }

    // ==================== exports ====================

    window.CraftOwned = {
        computeOwnedAllocation,
        invalidateOwnedAllocation,
        getOwnedAppliedQty,
        getRequiredQty,
        getOwnedMaterialsRawText,
        normalizeOwnedMaterialName,
        parseOwnedMaterialsInput,
        buildOwnedTypeLookupFromPayload,
        resolveSwitchState,
    };

    // Keep the historical global names working for craft_bp.js call sites.
    window.normalizeOwnedMaterialName = normalizeOwnedMaterialName;
    window.parseOwnedMaterialsInput = parseOwnedMaterialsInput;
    window.buildOwnedTypeLookupFromPayload = buildOwnedTypeLookupFromPayload;
})();
