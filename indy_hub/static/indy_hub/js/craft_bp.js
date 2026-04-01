/**
 * Craft Blueprint JavaScript functionality
 * Handles financial calculations, price fetching, and UI interactions
 */

// Global configuration
const CRAFT_BP = {
    fuzzworkUrl: null, // Will be set from Django template
    productTypeId: null, // Will be set from Django template
};
let CRAFT_COMPUTED_NEEDED_ROWS = null;
const CRAFT_BPC_CONTRACT_STATE = {
    loaded: false,
    loading: false,
    offersByBlueprintType: new Map(),
    selectedByBlueprintType: new Map(),
};
const CRAFT_INDUSTRY_FEE_STATE = {
    loading: false,
    signature: '',
    loaded: false,
    totalJobCost: 0,
    totalApiCost: 0,
    jobs: [],
    errors: [],
};
const CRAFT_BUILD_SYSTEM_STATE = {
    loading: false,
    lastSignature: '',
    debounceTimer: null,
    structuresById: new Map(),
};
const CRAFT_SELECTION_SCOPE = {
    initialized: false,
    blueprintTypeId: 0,
    simulationId: null,
    draftId: '',
};

const __ = (typeof window !== 'undefined' && typeof window.gettext === 'function') ? window.gettext.bind(window) : (msg => msg);

function craftBPIsDebugEnabled() {
    return (typeof window !== 'undefined' && window.INDY_HUB_DEBUG === true);
}

function craftBPDebugLog() {
    // Use console.log/info instead of console.debug so messages show up
    // in default Chrome/Firefox console filters.
    if (!craftBPIsDebugEnabled() || typeof console === 'undefined') {
        return;
    }

    if (typeof console.log === 'function') {
        console.log.apply(console, arguments);
        return;
    }
    if (typeof console.info === 'function') {
        console.info.apply(console, arguments);
    }
}

function updatePriceInputManualState(input, isManual) {
    if (!input) {
        return;
    }

    input.dataset.userModified = isManual ? 'true' : 'false';
    input.classList.toggle('is-manual', isManual);

    const cell = input.closest('td');
    if (cell) {
        cell.classList.toggle('has-manual', isManual);
    }

    const row = input.closest('tr');
    if (row) {
        const manualInRow = Array.from(row.querySelectorAll('.real-price, .sale-price-unit')).some(el => {
            if (el === input) {
                return isManual;
            }
            return el.dataset.userModified === 'true';
        });
        row.classList.toggle('has-manual', manualInRow);
        if (!manualInRow) {
            row.querySelectorAll('td.has-manual').forEach(td => td.classList.remove('has-manual'));
        }
    }
}

function escapeHtml(value) {
    if (value === null || value === undefined) {
        return '';
    }
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function formatInteger(value) {
    const num = Number(value) || 0;
    return num.toLocaleString();
}

function mapLikeToMap(source) {
    if (!source) {
        return new Map();
    }
    if (source instanceof Map) {
        return source;
    }
    if (Array.isArray(source)) {
        return new Map(source);
    }
    if (typeof source.entries === 'function') {
        try {
            return new Map(source.entries());
        } catch (error) {
            // Fall back to Object.entries below
        }
    }
    return new Map(Object.entries(source));
}

function getProductTypeIdValue() {
    const fromConfig = Number(CRAFT_BP.productTypeId);
    if (Number.isFinite(fromConfig) && fromConfig > 0) {
        return fromConfig;
    }
    const fromBlueprint = Number(window.BLUEPRINT_DATA?.product_type_id || window.BLUEPRINT_DATA?.productTypeId || 0);
    return Number.isFinite(fromBlueprint) ? fromBlueprint : 0;
}

function getSimulationPricesMap() {
    if (!window.SimulationAPI || typeof window.SimulationAPI.getState !== 'function') {
        return new Map();
    }
    const state = window.SimulationAPI.getState();
    if (!state || !state.prices) {
        return new Map();
    }
    return mapLikeToMap(state.prices);
}

function getCurrentQueryParam(name) {
    try {
        const params = new URLSearchParams(window.location.search || '');
        return String(params.get(name) || '').trim();
    } catch (error) {
        return '';
    }
}

function sanitizeScopeToken(rawValue) {
    return String(rawValue || '').trim().replace(/[^A-Za-z0-9_-]/g, '').slice(0, 64);
}

function createDraftScopeToken() {
    return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`;
}

function ensureCraftSelectionScope() {
    if (CRAFT_SELECTION_SCOPE.initialized) {
        return CRAFT_SELECTION_SCOPE;
    }

    const blueprintTypeId = Number(getCurrentBlueprintTypeId()) || 0;
    const simRaw = getCurrentQueryParam('sim');
    const simNumeric = Number(simRaw);
    if (Number.isFinite(simNumeric) && simNumeric > 0) {
        CRAFT_SELECTION_SCOPE.initialized = true;
        CRAFT_SELECTION_SCOPE.blueprintTypeId = blueprintTypeId;
        CRAFT_SELECTION_SCOPE.simulationId = Math.floor(simNumeric);
        CRAFT_SELECTION_SCOPE.draftId = '';
        return CRAFT_SELECTION_SCOPE;
    }

    let draftId = sanitizeScopeToken(getCurrentQueryParam('draft'));
    if (!draftId) {
        draftId = createDraftScopeToken();
        try {
            const nextUrl = new URL(window.location.href);
            nextUrl.searchParams.set('draft', draftId);
            window.history.replaceState(window.history.state, '', nextUrl.toString());
        } catch (error) {
            // Ignore URL sync failures and continue with in-memory draft scope.
        }
    }

    CRAFT_SELECTION_SCOPE.initialized = true;
    CRAFT_SELECTION_SCOPE.blueprintTypeId = blueprintTypeId;
    CRAFT_SELECTION_SCOPE.simulationId = null;
    CRAFT_SELECTION_SCOPE.draftId = draftId;
    return CRAFT_SELECTION_SCOPE;
}

function resetCraftSelectionScope() {
    CRAFT_SELECTION_SCOPE.initialized = false;
}

function getCraftSelectionScopeSuffix() {
    const scope = ensureCraftSelectionScope();
    if (scope.simulationId) {
        return `sim_${scope.simulationId}`;
    }
    const draftId = sanitizeScopeToken(scope.draftId) || 'default';
    return `draft_${draftId}`;
}

function getScopedCraftStorageKey(prefix) {
    const scope = ensureCraftSelectionScope();
    return `${prefix}_${String(scope.blueprintTypeId || 0)}_${getCraftSelectionScopeSuffix()}`;
}

function setCraftSimulationScope(simulationId) {
    const numericSimulationId = Number(simulationId) || 0;
    if (numericSimulationId <= 0) {
        return;
    }
    try {
        const nextUrl = new URL(window.location.href);
        nextUrl.searchParams.set('sim', String(Math.floor(numericSimulationId)));
        nextUrl.searchParams.delete('draft');
        window.history.replaceState(window.history.state, '', nextUrl.toString());
    } catch (error) {
        console.error('[CraftBP] Failed updating simulation scope URL', error);
    }
    resetCraftSelectionScope();
    ensureCraftSelectionScope();
}

function getBpcContractStorageKey() {
    return getScopedCraftStorageKey('craft_bp_bpc_contracts');
}

function loadSelectedBpcContractsFromStorage() {
    const storageKey = getBpcContractStorageKey();
    CRAFT_BPC_CONTRACT_STATE.selectedByBlueprintType = new Map();

    try {
        const raw = localStorage.getItem(storageKey);
        if (!raw) {
            return;
        }
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== 'object') {
            return;
        }

        Object.entries(parsed).forEach(([bpTypeIdRaw, offersRaw]) => {
            const blueprintTypeId = Number(bpTypeIdRaw) || 0;
            if (!blueprintTypeId || !Array.isArray(offersRaw)) {
                return;
            }
            const offerMap = new Map();
            offersRaw.forEach((offerRaw) => {
                const contractId = Number(offerRaw && offerRaw.contract_id) || 0;
                if (!contractId) {
                    return;
                }
                const normalized = {
                    blueprint_type_id: blueprintTypeId,
                    contract_id: contractId,
                    title: String(offerRaw.title || '').trim(),
                    price_total: Number(offerRaw.price_total) || 0,
                    price_per_run: Number(offerRaw.price_per_run) || 0,
                    runs: Math.max(1, Number(offerRaw.runs) || 1),
                    copies: Math.max(1, Number(offerRaw.copies) || 1),
                    me: Number(offerRaw.me) || 0,
                    te: Number(offerRaw.te) || 0,
                    issued_at: String(offerRaw.issued_at || ''),
                    expires_at: String(offerRaw.expires_at || ''),
                    selected_at: Number(offerRaw.selected_at) || Date.now(),
                };
                offerMap.set(contractId, normalized);
            });
            if (offerMap.size > 0) {
                CRAFT_BPC_CONTRACT_STATE.selectedByBlueprintType.set(blueprintTypeId, offerMap);
            }
        });
    } catch (error) {
        console.error('[CraftBP] Failed to load selected BPC contracts from storage', error);
    }
}

function saveSelectedBpcContractsToStorage() {
    const storageKey = getBpcContractStorageKey();
    const payload = {};
    CRAFT_BPC_CONTRACT_STATE.selectedByBlueprintType.forEach((offerMap, blueprintTypeId) => {
        if (!(offerMap instanceof Map) || offerMap.size === 0) {
            return;
        }
        payload[String(blueprintTypeId)] = Array.from(offerMap.values());
    });
    try {
        localStorage.setItem(storageKey, JSON.stringify(payload));
    } catch (error) {
        console.error('[CraftBP] Failed to persist selected BPC contracts', error);
    }
}

function getSelectedContractOffersForBlueprint(blueprintTypeId) {
    const numericBlueprintTypeId = Number(blueprintTypeId) || 0;
    if (!numericBlueprintTypeId) {
        return [];
    }
    const offerMap = CRAFT_BPC_CONTRACT_STATE.selectedByBlueprintType.get(numericBlueprintTypeId);
    if (!(offerMap instanceof Map) || offerMap.size === 0) {
        return [];
    }
    return Array.from(offerMap.values()).sort((left, right) => (Number(left.selected_at) || 0) - (Number(right.selected_at) || 0));
}

function getSelectedContractRunsForBlueprint(blueprintTypeId) {
    return getSelectedContractOffersForBlueprint(blueprintTypeId).reduce((total, offer) => {
        return total + Math.max(0, Number(offer.runs) || 0);
    }, 0);
}

function upsertSelectedContractOffer(blueprintTypeId, offer) {
    const numericBlueprintTypeId = Number(blueprintTypeId) || 0;
    const contractId = Number(offer && offer.contract_id) || 0;
    if (!numericBlueprintTypeId || !contractId) {
        return false;
    }
    let offerMap = CRAFT_BPC_CONTRACT_STATE.selectedByBlueprintType.get(numericBlueprintTypeId);
    if (!(offerMap instanceof Map)) {
        offerMap = new Map();
        CRAFT_BPC_CONTRACT_STATE.selectedByBlueprintType.set(numericBlueprintTypeId, offerMap);
    }
    offerMap.set(contractId, {
        blueprint_type_id: numericBlueprintTypeId,
        contract_id: contractId,
        title: String(offer.title || '').trim(),
        price_total: Number(offer.price_total) || 0,
        price_per_run: Number(offer.price_per_run) || 0,
        runs: Math.max(1, Number(offer.runs) || 1),
        copies: Math.max(1, Number(offer.copies) || 1),
        me: Number(offer.me) || 0,
        te: Number(offer.te) || 0,
        issued_at: String(offer.issued_at || ''),
        expires_at: String(offer.expires_at || ''),
        selected_at: Date.now(),
    });
    return true;
}

function removeSelectedContractOffer(blueprintTypeId, contractId) {
    const numericBlueprintTypeId = Number(blueprintTypeId) || 0;
    const numericContractId = Number(contractId) || 0;
    if (!numericBlueprintTypeId || !numericContractId) {
        return false;
    }
    const offerMap = CRAFT_BPC_CONTRACT_STATE.selectedByBlueprintType.get(numericBlueprintTypeId);
    if (!(offerMap instanceof Map)) {
        return false;
    }
    const removed = offerMap.delete(numericContractId);
    if (offerMap.size === 0) {
        CRAFT_BPC_CONTRACT_STATE.selectedByBlueprintType.delete(numericBlueprintTypeId);
    }
    return removed;
}

function attachPriceInputListener(input) {
    if (!input || input.dataset.priceListenerAttached === 'true') {
        return;
    }

    input.addEventListener('input', () => {
        updatePriceInputManualState(input, true);

        if (window.SimulationAPI && typeof window.SimulationAPI.setPrice === 'function') {
            const typeId = input.getAttribute('data-type-id');
            if (typeId) {
                const priceType = input.classList.contains('sale-price-unit') ? 'sale' : 'real';
                window.SimulationAPI.setPrice(typeId, priceType, parseFloat(input.value) || 0);
            }
        }

        if (typeof recalcFinancials === 'function') {
            recalcFinancials();
        }
    });

    input.dataset.priceListenerAttached = 'true';
}

function refreshTabsAfterStateChange(options = {}) {
    if (!options.keepComputedNeeded) {
        CRAFT_COMPUTED_NEEDED_ROWS = null;
    }
    if (typeof syncConfigureVisibilityWithPlan === 'function') {
        syncConfigureVisibilityWithPlan();
    }
    if (typeof updateMaterialsTabFromState === 'function') {
        updateMaterialsTabFromState();
    }
    if (typeof updateFinancialTabFromState === 'function') {
        updateFinancialTabFromState();
    }
    if (typeof updateNeededTabFromState === 'function') {
        updateNeededTabFromState(Boolean(options.forceNeeded));
    }
    if (typeof renderBuyBpcsTab === 'function') {
        renderBuyBpcsTab();
    }
}

/**
 * Public API for configuration
 */
window.CraftBP = {
    init: function(config) {
        CRAFT_BP.fuzzworkUrl = config.fuzzworkPriceUrl;
        CRAFT_BP.productTypeId = config.productTypeId;

        // Initialize financial calculations after configuration
        initializeFinancialCalculations();
    },

    loadFuzzworkPrices: function(typeIds) {
        return fetchAllPrices(typeIds);
    },

    refreshFinancials: function() {
        if (window.SimulationAPI && typeof window.SimulationAPI.refreshFromDom === 'function') {
            window.SimulationAPI.refreshFromDom();
        }
        recalcFinancials();
    },

    refreshTabs: function(options = {}) {
        refreshTabsAfterStateChange(options);
    },

    markPriceOverride: function(element, isManual = true) {
        updatePriceInputManualState(element, isManual);
    },

    pushStatus: function(message, variant = 'info') {
        const event = new CustomEvent('CraftBP:status', {
            detail: {
                message,
                variant
            }
        });
        document.dispatchEvent(event);
    },

    setSimulationScope: function(simulationId, options = {}) {
        const migrateCurrentState = options.migrateCurrentState !== false;
        setCraftSimulationScope(simulationId);

        if (migrateCurrentState) {
            if (typeof getCurrentMETEConfig === 'function') {
                const storageKey = getScopedCraftStorageKey('craft_bp_config');
                try {
                    localStorage.setItem(storageKey, JSON.stringify(getCurrentMETEConfig()));
                } catch (error) {
                    console.error('[CraftBP] Failed to persist configure state for simulation scope', error);
                }
            }
            if (typeof saveSelectedBpcContractsToStorage === 'function') {
                saveSelectedBpcContractsToStorage();
            }
            return;
        }

        if (typeof loadSelectedBpcContractsFromStorage === 'function') {
            loadSelectedBpcContractsFromStorage();
        }
        if (typeof renderBuyBpcsTab === 'function') {
            renderBuyBpcsTab();
        }
    }
};

/**
 * Initialize the application
 */
document.addEventListener('DOMContentLoaded', function() {
    // Capture the initial dashboard Materials ordering before any UI updates replace the markup.
    try {
        getDashboardMaterialsOrdering();
    } catch (e) {
        // ignore
    }

    initializeBlueprintIcons();
    initializeCollapseHandlers();
    ensureCraftSelectionScope();
    initializeBuyCraftSwitches();
    restoreBuyCraftStateFromURL();
    loadSelectedBpcContractsFromStorage();
    initializeBuyBpcsTab();
    syncConfigureVisibilityWithPlan();
    if (window.SimulationAPI && typeof window.SimulationAPI.refreshFromDom === 'function') {
        window.SimulationAPI.refreshFromDom();
    }
    if (typeof updateMaterialsTabFromState === 'function') {
        updateMaterialsTabFromState();
    }
    initializeRunOptimizedTab();
    // Financial calculations will be initialized via CraftBP.init()
});

/**
 * Initialize blueprint icon error handling
 */
function initializeBlueprintIcons() {
    document.querySelectorAll('.blueprint-icon img').forEach(function(img) {
        img.onerror = function() {
            this.style.display = 'none';
            if (this.nextElementSibling) {
                this.nextElementSibling.style.display = 'flex';
            }
        };
    });
}

/**
 * Initialize buy/craft switch handlers for material tree
 * DISABLED - Now handled by template event listeners to prevent page reloads
 */
function initializeBuyCraftSwitches() {
    const treeTab = document.getElementById('tab-tree');
    if (!treeTab) {
        console.warn('Tree tab not found; skipping buy/craft switch initialization');
        return;
    }

    if (treeTab.dataset.switchesInitialized === 'true') {
        refreshTreeSwitchHierarchy();
        return;
    }
    treeTab.dataset.switchesInitialized = 'true';

    window.refreshTreeSwitchHierarchy = refreshTreeSwitchHierarchy;

    const switches = Array.from(treeTab.querySelectorAll('input.mat-switch'));
    switches.forEach(sw => {
        if (!sw.dataset.userState) {
            if (sw.disabled && sw.closest('.mat-switch-group')?.querySelector('.mode-label')?.textContent?.trim().toLowerCase() === 'useless') {
                sw.dataset.userState = 'useless';
                sw.dataset.fixedMode = 'useless';
            } else {
                sw.dataset.userState = sw.checked ? 'prod' : 'buy';
            }
        }
        if (!sw.dataset.parentLockDepth) {
            sw.dataset.parentLockDepth = '0';
        }
        if (!sw.dataset.lockedByParent) {
            sw.dataset.lockedByParent = 'false';
        }
        if (!sw.dataset.initialUserDisabled) {
            sw.dataset.initialUserDisabled = sw.disabled ? 'true' : 'false';
        }
        updateSwitchLabel(sw);
    });

    refreshTreeSwitchHierarchy();

    treeTab.addEventListener('change', handleTreeSwitchChange, true);
}

function shouldApplySwitchSelectionToAllInstances() {
    const checkbox = document.getElementById('applySwitchToAllInstances');
    return Boolean(checkbox && checkbox.checked);
}

function applySwitchStateToTypeInstances(typeId, state) {
    const treeTab = document.getElementById('tab-tree');
    if (!treeTab) {
        return;
    }

    const numericTypeId = Number(typeId) || 0;
    if (!numericTypeId) {
        return;
    }

    const desiredState = state === 'buy' ? 'buy' : 'prod';
    const matches = Array.from(
        treeTab.querySelectorAll(`input.mat-switch[data-type-id="${numericTypeId}"]`)
    );

    matches.forEach((switchEl) => {
        if (!switchEl || switchEl.dataset.fixedMode === 'useless') {
            return;
        }
        switchEl.dataset.userState = desiredState;
        switchEl.checked = desiredState !== 'buy';
        updateSwitchLabel(switchEl);
    });
}

function handleTreeSwitchChange(event) {
    const switchEl = event.target;
    if (!switchEl || !switchEl.classList || !switchEl.classList.contains('mat-switch')) {
        return;
    }

    if (switchEl.disabled || switchEl.dataset.fixedMode === 'useless') {
        event.preventDefault();
        return;
    }

    const newState = switchEl.checked ? 'prod' : 'buy';
    const typeId = Number(switchEl.getAttribute('data-type-id') || 0);

    if (shouldApplySwitchSelectionToAllInstances() && typeId > 0) {
        applySwitchStateToTypeInstances(typeId, newState);
    } else {
        switchEl.dataset.userState = newState;
        updateSwitchLabel(switchEl);
    }

    refreshTreeSwitchHierarchy();

    if (window.SimulationAPI && typeof window.SimulationAPI.refreshFromDom === 'function') {
        window.SimulationAPI.refreshFromDom();
    }

    refreshTabsAfterStateChange();
}

function refreshTreeSwitchHierarchy() {
    const treeTab = document.getElementById('tab-tree');
    if (!treeTab) {
        return;
    }

    const switches = Array.from(treeTab.querySelectorAll('input.mat-switch'));
    switches.forEach(applyParentLockState);
    refreshTreeBlueprintContextLabels();
    refreshTreeQuantityLabels();
}

if (typeof window !== 'undefined' && !window.refreshTreeSwitchHierarchy) {
    window.refreshTreeSwitchHierarchy = refreshTreeSwitchHierarchy;
}

function applyParentLockState(switchEl) {
    const group = switchEl.closest('.mat-switch-group');
    const toggleContainer = group ? group.querySelector('.form-switch') : null;
    const isFixedUseless = switchEl.dataset.fixedMode === 'useless' || switchEl.dataset.userState === 'useless';
    if (isFixedUseless) {
        switchEl.disabled = true;
        switchEl.checked = false;
        switchEl.dataset.lockedByParent = 'false';
        switchEl.dataset.parentLockDepth = '0';
        if (toggleContainer) {
            toggleContainer.classList.add('d-none');
        }
        updateSwitchLabel(switchEl);
        return;
    }

    const ancestorBuyCount = countBuyAncestors(switchEl);
    if (ancestorBuyCount > 0) {
        switchEl.disabled = true;
        switchEl.checked = false;
        switchEl.dataset.lockedByParent = 'true';
        switchEl.dataset.parentLockDepth = String(ancestorBuyCount);
        if (toggleContainer) {
            toggleContainer.classList.add('d-none');
        }
    } else {
        const desiredState = switchEl.dataset.userState || (switchEl.checked ? 'prod' : 'buy');
        switchEl.disabled = false;
        switchEl.dataset.lockedByParent = 'false';
        switchEl.dataset.parentLockDepth = '0';
        switchEl.checked = desiredState !== 'buy';
        if (toggleContainer) {
            toggleContainer.classList.remove('d-none');
        }
    }

    updateSwitchLabel(switchEl);
}

function countBuyAncestors(switchEl) {
    let count = 0;
    let currentDetail = switchEl.closest('details');
    if (!currentDetail) {
        return 0;
    }

    currentDetail = currentDetail.parentElement ? currentDetail.parentElement.closest('details') : null;
    while (currentDetail) {
        const ancestorSwitch = currentDetail.querySelector('summary input.mat-switch');
        if (ancestorSwitch) {
            const ancestorMode = ancestorSwitch.dataset.fixedMode;
            const ancestorForced = ancestorSwitch.dataset.lockedByParent === 'true';
            const ancestorIsBuy = (!ancestorSwitch.checked) || ancestorMode === 'useless';
            if (ancestorIsBuy || ancestorForced) {
                count += 1;
            }
        }
        currentDetail = currentDetail.parentElement ? currentDetail.parentElement.closest('details') : null;
    }

    return count;
}

function updateDetailsCaret(detailsEl) {
    if (!detailsEl) {
        return;
    }
    const icon = detailsEl.querySelector(':scope > summary .summary-icon i');
    if (!icon) {
        return;
    }
    icon.classList.remove('fa-caret-right', 'fa-caret-down');
    icon.classList.add(detailsEl.open ? 'fa-caret-down' : 'fa-caret-right');
}

function refreshTreeSummaryIcons() {
    const treeTab = document.getElementById('tab-tree');
    if (!treeTab) {
        return;
    }
    treeTab.querySelectorAll('details').forEach(updateDetailsCaret);
}

function expandAllTreeNodes() {
    const treeTab = document.getElementById('tab-tree');
    if (!treeTab) {
        return;
    }
    treeTab.querySelectorAll('details').forEach(detailsEl => {
        if (!detailsEl.open) {
            detailsEl.open = true;
        }
        updateDetailsCaret(detailsEl);
    });
}

function collapseAllTreeNodes() {
    const treeTab = document.getElementById('tab-tree');
    if (!treeTab) {
        return;
    }
    treeTab.querySelectorAll('details').forEach(detailsEl => {
        if (detailsEl.open) {
            detailsEl.open = false;
        }
        updateDetailsCaret(detailsEl);
    });
}

function setTreeModeForAll(mode) {
    const treeTab = document.getElementById('tab-tree');
    if (!treeTab) {
        return;
    }

    const desiredState = mode === 'buy' ? 'buy' : 'prod';
    const switches = Array.from(treeTab.querySelectorAll('input.mat-switch'));

    switches.forEach(sw => {
        if (sw.dataset.fixedMode === 'useless') {
            return;
        }
        sw.dataset.userState = desiredState;
        sw.checked = desiredState !== 'buy';
    });

    refreshTreeSwitchHierarchy();
    if (window.SimulationAPI && typeof window.SimulationAPI.refreshFromDom === 'function') {
        window.SimulationAPI.refreshFromDom();
    }

    refreshTabsAfterStateChange();
}

async function optimizeProfitabilityConfig() {
    // Heuristic optimizer: choose Buy vs Prod per craftable node by comparing
    // buy cost vs best sub-tree production cost (including surplus credit).
    // Uses current run count + ME/TE because those are already baked into materials_tree quantities.

    const tree = window.BLUEPRINT_DATA?.materials_tree;
    if (!Array.isArray(tree) || tree.length === 0) {
        if (window.CraftBP && typeof window.CraftBP.pushStatus === 'function') {
            window.CraftBP.pushStatus(__('No production tree to optimize'), 'warning');
        }
        return;
    }

    if (!window.SimulationAPI || typeof window.SimulationAPI.getPrice !== 'function' || typeof window.SimulationAPI.setPrice !== 'function') {
        if (window.CraftBP && typeof window.CraftBP.pushStatus === 'function') {
            window.CraftBP.pushStatus(__('Prices are not ready yet'), 'warning');
        }
        return;
    }

    // Ensure we read any manual overrides already present in the DOM.
    if (typeof window.SimulationAPI.refreshFromDom === 'function') {
        window.SimulationAPI.refreshFromDom();
    }

    // Preload buy prices for every node in the tree.
    // This avoids optimizing with missing prices (which can incorrectly bias toward PROD).
    function collectTypeIds(nodes, out = new Set()) {
        (Array.isArray(nodes) ? nodes : []).forEach(node => {
            const tid = Number(node?.type_id || node?.typeId) || 0;
            if (tid > 0) {
                out.add(String(tid));
            }
            const kids = node && (node.sub_materials || node.subMaterials);
            if (Array.isArray(kids) && kids.length) {
                collectTypeIds(kids, out);
            }
        });
        return out;
    }

    const allTypeIds = Array.from(collectTypeIds(tree));
    if (typeof fetchAllPrices === 'function' && allTypeIds.length > 0) {
        try {
            const optimizeBtn = document.getElementById('optimize-profit');
            if (optimizeBtn) {
                optimizeBtn.disabled = true;
            }
            if (window.CraftBP && typeof window.CraftBP.pushStatus === 'function') {
                window.CraftBP.pushStatus(__('Loading market prices for optimization…'), 'info');
            }

            const prices = await fetchAllPrices(allTypeIds);
            // Stash fuzzwork prices so getBuyPrice can fall back to them.
            allTypeIds.forEach(tid => {
                const raw = prices[tid] ?? prices[String(parseInt(tid, 10))];
                const price = raw != null ? (parseFloat(raw) || 0) : 0;
                if (price > 0) {
                    window.SimulationAPI.setPrice(tid, 'fuzzwork', price);
                }
            });

            // Re-read DOM again so manual overrides (real/sale) keep priority.
            if (typeof window.SimulationAPI.refreshFromDom === 'function') {
                window.SimulationAPI.refreshFromDom();
            }
            if (optimizeBtn) {
                optimizeBtn.disabled = false;
            }
        } catch (error) {
            const optimizeBtn = document.getElementById('optimize-profit');
            if (optimizeBtn) {
                optimizeBtn.disabled = false;
            }
            if (window.CraftBP && typeof window.CraftBP.pushStatus === 'function') {
                window.CraftBP.pushStatus(__('Failed to load prices for optimization'), 'warning');
            }
        }
    }

    const productTypeId = Number(CRAFT_BP.productTypeId) || 0;

    // For optimization we need to distinguish BUY vs SELL prices.
    // BUY: prefer real (manual buy override) then fuzzwork, never fall back to sale.
    // SELL: prefer sale (manual sell override) then fuzzwork, then real.
    function getBuyUnitPrice(typeId) {
        const state = (typeof window.SimulationAPI.getState === 'function') ? window.SimulationAPI.getState() : null;
        const prices = state && state.prices ? state.prices : null;
        const record = prices && (prices instanceof Map ? prices.get(Number(typeId)) : prices[Number(typeId)]);
        const real = record ? (Number(record.real) || 0) : 0;
        if (real > 0) return real;
        const fuzz = record ? (Number(record.fuzzwork) || 0) : 0;
        if (fuzz > 0) return fuzz;
        return 0;
    }

    function getSellUnitPrice(typeId) {
        const state = (typeof window.SimulationAPI.getState === 'function') ? window.SimulationAPI.getState() : null;
        const prices = state && state.prices ? state.prices : null;
        const record = prices && (prices instanceof Map ? prices.get(Number(typeId)) : prices[Number(typeId)]);
        const sale = record ? (Number(record.sale) || 0) : 0;
        if (sale > 0) return sale;
        const fuzz = record ? (Number(record.fuzzwork) || 0) : 0;
        if (fuzz > 0) return fuzz;
        const real = record ? (Number(record.real) || 0) : 0;
        if (real > 0) return real;
        return 0;
    }

    function getBuyUnitPriceOrInf(typeId) {
        const p = getBuyUnitPrice(typeId);
        return p > 0 ? p : Number.POSITIVE_INFINITY;
    }

    function readChildren(node) {
        const kids = node && (node.sub_materials || node.subMaterials);
        return Array.isArray(kids) ? kids : [];
    }

    function readTypeId(node) {
        return Number(node?.type_id || node?.typeId) || 0;
    }

    function readQty(node) {
        const q = Number(node?.quantity ?? node?.qty ?? 0);
        return Number.isFinite(q) ? Math.max(0, Math.ceil(q)) : 0;
    }

    function readProducedPerCycle(node) {
        const p = Number(node?.produced_per_cycle ?? node?.producedPerCycle ?? 0);
        return Number.isFinite(p) ? Math.max(0, Math.ceil(p)) : 0;
    }

    // --- Global aggregated optimizer (handles shared children + cycle rounding economies of scale) ---
    // Build per-type recipes from the expanded materials_tree.
    // For a craftable typeId, a recipe is defined by produced_per_cycle and input quantities per *cycle*.
    const occurrencesByType = new Map();
    const nameByType = new Map();
    (function collectOccurrences(nodes) {
        (Array.isArray(nodes) ? nodes : []).forEach(node => {
            const typeId = readTypeId(node);
            if (typeId) {
                const typeName = node?.type_name || node?.typeName || '';
                if (typeName && !nameByType.has(typeId)) {
                    nameByType.set(typeId, typeName);
                }
            }

            const children = readChildren(node);
            if (typeId && children.length > 0) {
                if (!occurrencesByType.has(typeId)) {
                    occurrencesByType.set(typeId, []);
                }
                occurrencesByType.get(typeId).push(node);
            }

            if (children.length > 0) {
                collectOccurrences(children);
            }
        });
    })(tree);

    const recipes = new Map(); // typeId -> { producedPerCycle, inputsPerCycle: Map<childTypeId, perCycleQty> }
    occurrencesByType.forEach((nodes, typeId) => {
        // Choose an occurrence with the largest cycle count for stability.
        let best = null;
        let bestCycles = 0;
        nodes.forEach(n => {
            const ppc = readProducedPerCycle(n);
            const needed = readQty(n);
            if (!ppc || !needed) return;
            const cycles = Math.max(1, Math.ceil(needed / ppc));
            if (cycles >= bestCycles) {
                bestCycles = cycles;
                best = n;
            }
        });

        if (!best) return;
        const ppc = readProducedPerCycle(best);
        const needed = readQty(best);
        if (!ppc || !needed) return;
        const cycles = Math.max(1, Math.ceil(needed / ppc));

        const inputsPerCycle = new Map();
        readChildren(best).forEach(child => {
            const childTypeId = readTypeId(child);
            if (!childTypeId) return;
            const childQty = readQty(child);
            if (!childQty) return;
            inputsPerCycle.set(childTypeId, childQty / cycles);
        });
        recipes.set(typeId, { producedPerCycle: ppc, inputsPerCycle });
    });

    // Seed decisions from current switch states (keeps optimizer deterministic for the user).
    const decisions = new Map(); // typeId -> 'buy' | 'prod'
    document.querySelectorAll('#tab-tree input.mat-switch[data-type-id]').forEach(sw => {
        const id = Number(sw.getAttribute('data-type-id')) || 0;
        if (!id) return;
        if (sw.dataset.fixedMode === 'useless' || sw.dataset.userState === 'useless') return;
        decisions.set(id, sw.checked ? 'prod' : 'buy');
    });

    // Top-level requirements (materials needed for the final product).
    const rootDemand = new Map();
    tree.forEach(rootNode => {
        const id = readTypeId(rootNode);
        if (!id) return;
        if (productTypeId && id === productTypeId) return;
        const q = readQty(rootNode);
        if (!q) return;
        rootDemand.set(id, (rootDemand.get(id) || 0) + q);
    });

    // Build dependency graph parent -> child (only for craftables we have recipes for).
    const craftables = new Set(recipes.keys());
    const edges = new Map();
    const indegree = new Map();
    craftables.forEach(id => {
        edges.set(id, new Set());
        indegree.set(id, 0);
    });

    recipes.forEach((rec, parentId) => {
        rec.inputsPerCycle.forEach((_, childId) => {
            if (!craftables.has(parentId)) return;
            if (!edges.has(parentId)) edges.set(parentId, new Set());
            edges.get(parentId).add(childId);
            if (craftables.has(childId)) {
                indegree.set(childId, (indegree.get(childId) || 0) + 1);
            }
        });
    });

    // Kahn topo order: parents before children.
    const queue = [];
    indegree.forEach((deg, id) => {
        if (deg === 0) queue.push(id);
    });
    const topo = [];
    while (queue.length) {
        const id = queue.shift();
        topo.push(id);
        (edges.get(id) || new Set()).forEach(childId => {
            if (!craftables.has(childId)) return;
            const nextDeg = (indegree.get(childId) || 0) - 1;
            indegree.set(childId, nextDeg);
            if (nextDeg === 0) queue.push(childId);
        });
    }

    // Demand propagation in topo order so shared children aggregate before cycle rounding.
    function computeDemand(currentDecisions) {
        const demand = new Map(rootDemand);
        topo.forEach(typeId => {
            if (!craftables.has(typeId)) return;
            if ((currentDecisions.get(typeId) || 'prod') !== 'prod') return;
            const needed = demand.get(typeId) || 0;
            if (needed <= 0) return;
            const rec = recipes.get(typeId);
            if (!rec || !rec.producedPerCycle) return;
            const cycles = Math.max(1, Math.ceil(needed / rec.producedPerCycle));
            rec.inputsPerCycle.forEach((perCycleQty, childId) => {
                // Keep demand integer-safe; avoid float noise accumulation.
                const add = Math.max(0, Math.ceil((perCycleQty * cycles) - 1e-9));
                if (add <= 0) return;
                demand.set(childId, (demand.get(childId) || 0) + add);
            });
        });
        return demand;
    }

    // Compute best unit costs bottom-up for a given demand snapshot.
    function computeBestUnitCosts(demand, currentDecisions) {
        const bestUnitCost = new Map();
        const chosenMode = new Map();
        const reverseTopo = topo.slice().reverse();

        reverseTopo.forEach(typeId => {
            const needed = demand.get(typeId) || 0;
            if (!craftables.has(typeId) || needed <= 0) {
                return;
            }

            const buyUnit = getBuyUnitPriceOrInf(typeId);
            const buyTotal = buyUnit * needed;

            const rec = recipes.get(typeId);
            if (!rec || !rec.producedPerCycle) {
                bestUnitCost.set(typeId, buyUnit > 0 ? buyUnit : 0);
                chosenMode.set(typeId, 'buy');
                return;
            }

            const cycles = Math.max(1, Math.ceil(needed / rec.producedPerCycle));
            const produced = cycles * rec.producedPerCycle;
            const surplus = Math.max(0, produced - needed);

            let inputsCost = 0;
            rec.inputsPerCycle.forEach((perCycleQty, childId) => {
                const childQtyTotal = Math.max(0, Math.ceil((perCycleQty * cycles) - 1e-9));
                if (childQtyTotal <= 0) return;
                const childIsCraftable = craftables.has(childId);
                const childUnit = childIsCraftable
                    ? (bestUnitCost.get(childId) ?? getBuyUnitPriceOrInf(childId))
                    : getBuyUnitPriceOrInf(childId);
                inputsCost += childUnit * childQtyTotal;
            });

            const sellUnit = getSellUnitPrice(typeId);
            const credit = (sellUnit > 0 ? sellUnit : 0) * surplus;
            const prodTotal = inputsCost - credit;
            const prodUnit = needed > 0 ? (prodTotal / needed) : Number.POSITIVE_INFINITY;

            // Choose best mode for this demand snapshot.
            let mode;
            if (!Number.isFinite(prodTotal) && !Number.isFinite(buyTotal)) {
                mode = currentDecisions.get(typeId) || 'prod';
            } else {
                mode = (prodTotal <= buyTotal) ? 'prod' : 'buy';
            }
            chosenMode.set(typeId, mode);
            bestUnitCost.set(typeId, mode === 'prod' ? prodUnit : buyUnit);
        });

        // Keep existing decisions for types not in topo/demand.
        craftables.forEach(typeId => {
            if (!chosenMode.has(typeId)) {
                chosenMode.set(typeId, currentDecisions.get(typeId) || 'prod');
            }
        });

        return { bestUnitCost, chosenMode };
    }

    function computeCostsBreakdown(demand, currentDecisions) {
        const reverseTopo = topo.slice().reverse();
        const bestUnitCost = new Map();
        const chosenMode = new Map();
        const breakdown = new Map();

        reverseTopo.forEach(typeId => {
            const needed = demand.get(typeId) || 0;
            if (!craftables.has(typeId) || needed <= 0) {
                return;
            }

            const buyUnitRaw = getBuyUnitPrice(typeId);
            const buyUnit = getBuyUnitPriceOrInf(typeId);
            const buyTotal = buyUnit * needed;

            const rec = recipes.get(typeId);
            if (!rec || !rec.producedPerCycle) {
                chosenMode.set(typeId, Number.isFinite(buyTotal) ? 'buy' : (currentDecisions.get(typeId) || 'prod'));
                bestUnitCost.set(typeId, buyUnit);
                breakdown.set(typeId, {
                    typeId,
                    name: nameByType.get(typeId) || '',
                    needed,
                    buyUnit: buyUnitRaw,
                    buyTotal: Number.isFinite(buyTotal) ? buyTotal : null,
                    prodTotal: null,
                    prodUnit: null,
                    cycles: null,
                    produced: null,
                    surplus: null,
                    surplusCredit: null,
                    mode: chosenMode.get(typeId),
                });
                return;
            }

            const cycles = Math.max(1, Math.ceil(needed / rec.producedPerCycle));
            const produced = cycles * rec.producedPerCycle;
            const surplus = Math.max(0, produced - needed);

            let inputsCost = 0;
            rec.inputsPerCycle.forEach((perCycleQty, childId) => {
                const childQtyTotal = Math.max(0, Math.ceil((perCycleQty * cycles) - 1e-9));
                if (childQtyTotal <= 0) return;

                const childIsCraftable = craftables.has(childId);
                const childUnitCost = childIsCraftable
                    ? (bestUnitCost.get(childId) ?? getBuyUnitPriceOrInf(childId))
                    : getBuyUnitPriceOrInf(childId);
                inputsCost += childUnitCost * childQtyTotal;
            });

            const sellUnit = getSellUnitPrice(typeId);
            const surplusCredit = (sellUnit > 0 ? sellUnit : 0) * surplus;
            const prodTotal = inputsCost - surplusCredit;
            const prodUnit = needed > 0 ? (prodTotal / needed) : Number.POSITIVE_INFINITY;

            let mode;
            if (!Number.isFinite(prodTotal) && !Number.isFinite(buyTotal)) {
                mode = currentDecisions.get(typeId) || 'prod';
            } else {
                mode = (prodTotal <= buyTotal) ? 'prod' : 'buy';
            }

            chosenMode.set(typeId, mode);
            bestUnitCost.set(typeId, mode === 'prod' ? prodUnit : buyUnit);
            breakdown.set(typeId, {
                typeId,
                name: nameByType.get(typeId) || '',
                needed,
                buyUnit: buyUnitRaw,
                buyTotal: Number.isFinite(buyTotal) ? buyTotal : null,
                prodTotal: Number.isFinite(prodTotal) ? prodTotal : null,
                prodUnit: Number.isFinite(prodUnit) ? prodUnit : null,
                cycles,
                produced,
                surplus,
                surplusCredit: Number.isFinite(surplusCredit) ? surplusCredit : null,
                mode,
            });
        });

        return { breakdown, chosenMode, bestUnitCost };
    }

    // Iterate to stability: decisions influence demand (cycle rounding), which influences costs.
    let lastChangeCount = 0;
    for (let iter = 0; iter < 6; iter += 1) {
        const demand = computeDemand(decisions);
        const { chosenMode } = computeBestUnitCosts(demand, decisions);

        let changed = 0;
        chosenMode.forEach((mode, typeId) => {
            const prev = decisions.get(typeId) || 'prod';
            if (prev !== mode) {
                decisions.set(typeId, mode);
                changed += 1;
            }
        });
        lastChangeCount = changed;
        if (changed === 0) {
            break;
        }
    }

    const aggregateDecisions = decisions;

    // --- Margin-first refinement ---
    // The global optimizer above minimizes net production cost (incl. surplus credit) per node.
    // The user expectation is: optimize for best displayed margin (profit / revenue).
    // We therefore evaluate the same model used by the financial tab (financial items + final sale + surplus)
    // and greedily flip switches when it improves margin.
    function computeDisplayedMarginSnapshot() {
        const api = window.SimulationAPI;
        if (!api || typeof api.getFinancialItems !== 'function' || typeof api.getPrice !== 'function') {
            return { margin: Number.NEGATIVE_INFINITY, profit: 0, revenue: 0, cost: 0, surplusRevenue: 0 };
        }

        const productTypeIdLocal = Number(CRAFT_BP.productTypeId) || 0;

        // Cost: sum of buy items (real>fuzzwork) from financial items.
        let costTotal = 0;
        const items = api.getFinancialItems() || [];
        items.forEach(item => {
            const typeId = Number(item.typeId ?? item.type_id) || 0;
            if (!typeId || (productTypeIdLocal && typeId === productTypeIdLocal)) return;
            const qty = Math.max(0, Math.ceil(Number(item.quantity ?? item.qty ?? 0))) || 0;
            if (!qty) return;
            const unit = api.getPrice(typeId, 'buy');
            const unitPrice = unit && typeof unit.value === 'number' ? unit.value : 0;
            if (unitPrice > 0) costTotal += unitPrice * qty;
        });

        // Revenue: final product + surplus credit.
        let revenueTotal = 0;

        try {
            const finalRow = document.getElementById('finalProductRow');
            const finalQtyEl = finalRow ? finalRow.querySelector('[data-qty]') : null;
            const rawFinalQty = finalQtyEl ? (finalQtyEl.getAttribute('data-qty') || finalQtyEl.dataset?.qty) : null;
            const finalQty = Math.max(0, Math.ceil(Number(rawFinalQty))) || 0;

            if (productTypeIdLocal && finalQty > 0) {
                const unit = api.getPrice(productTypeIdLocal, 'sale');
                const unitPrice = unit && typeof unit.value === 'number' ? unit.value : 0;
                if (unitPrice > 0) {
                    revenueTotal += unitPrice * finalQty;
                }
            }
        } catch (e) {
            // ignore
        }

        let surplusRevenue = 0;
        try {
            const cycles = (typeof api.getProductionCycles === 'function') ? (api.getProductionCycles() || []) : [];
            if (Array.isArray(cycles) && cycles.length) {
                cycles.forEach(entry => {
                    const typeId = Number(entry.typeId || entry.type_id || 0) || 0;
                    const surplusQty = Number(entry.surplus) || 0;
                    if (!typeId || surplusQty <= 0) return;
                    if (productTypeIdLocal && typeId === productTypeIdLocal) return;
                    const unit = api.getPrice(typeId, 'sale');
                    const unitPrice = unit && typeof unit.value === 'number' ? unit.value : 0;
                    if (unitPrice > 0) {
                        surplusRevenue += unitPrice * surplusQty;
                    }
                });
            }
        } catch (e) {
            // ignore
        }

        revenueTotal += surplusRevenue;
        const profit = revenueTotal - costTotal;
        const margin = revenueTotal > 0 ? (profit / revenueTotal) : Number.NEGATIVE_INFINITY;
        return { margin, profit, revenue: revenueTotal, cost: costTotal, surplusRevenue };
    }

    // Apply current decisions into SimulationAPI so evaluation uses the right state.
    if (window.SimulationAPI && typeof window.SimulationAPI.setSwitchState === 'function') {
        aggregateDecisions.forEach((mode, typeId) => {
            window.SimulationAPI.setSwitchState(typeId, mode);
        });
    }

    // Greedy improvement: flip one switch at a time if it increases margin.
    // Keep iterations small to avoid UI freezes on large trees.
    try {
        const api = window.SimulationAPI;
        if (api && typeof api.setSwitchState === 'function' && typeof api.getSwitchState === 'function') {
            const candidates = Array.from(aggregateDecisions.keys());
            let best = computeDisplayedMarginSnapshot();
            const epsilon = 1e-9;

            for (let iter = 0; iter < 3; iter += 1) {
                let bestType = null;
                let bestNewState = null;
                let bestNew = null;
                let bestGain = 0;

                candidates.forEach(typeId => {
                    const current = api.getSwitchState(typeId) || (aggregateDecisions.get(typeId) || 'prod');
                    if (current !== 'buy' && current !== 'prod') return;
                    const trial = current === 'buy' ? 'prod' : 'buy';

                    api.setSwitchState(typeId, trial);
                    const snap = computeDisplayedMarginSnapshot();
                    const gain = snap.margin - best.margin;
                    api.setSwitchState(typeId, current);

                    if (gain > bestGain + epsilon) {
                        bestGain = gain;
                        bestType = typeId;
                        bestNewState = trial;
                        bestNew = snap;
                    }
                });

                if (!bestType || !bestNewState || !bestNew || bestGain <= epsilon) {
                    break;
                }

                api.setSwitchState(bestType, bestNewState);
                aggregateDecisions.set(bestType, bestNewState);
                best = bestNew;
            }
        }
    } catch (e) {
        console.warn('[IndyHub] Margin-first refinement failed', e);
    }

    // --- Debug dump (console) ---
    // Provides buy/prod totals per item so the user can paste the full dataset.
    try {
        const finalDemand = computeDemand(aggregateDecisions);
        const { breakdown } = computeCostsBreakdown(finalDemand, aggregateDecisions);

        const demandIds = Array.from(finalDemand.keys()).map(Number).filter(Boolean);
        demandIds.sort((a, b) => a - b);

        const rows = demandIds.map(typeId => {
            const needed = finalDemand.get(typeId) || 0;
            const buyUnitRaw = getBuyUnitPrice(typeId);
            const buyTotal = buyUnitRaw > 0 ? (buyUnitRaw * needed) : null;
            const craftRow = breakdown.get(typeId);

            return {
                typeId,
                name: craftRow?.name || nameByType.get(typeId) || '',
                needed,
                buyUnit: buyUnitRaw || null,
                buyTotal,
                prodTotal: craftRow?.prodTotal ?? null,
                prodUnit: craftRow?.prodUnit ?? null,
                cycles: craftRow?.cycles ?? null,
                produced: craftRow?.produced ?? null,
                surplus: craftRow?.surplus ?? null,
                surplusCredit: craftRow?.surplusCredit ?? null,
                mode: aggregateDecisions.get(typeId) || craftRow?.mode || null,
            };
        });

        // Persist the full dataset for copy/paste.
        window.__IndyHubOptimizeDebug = {
            productTypeId,
            generatedAt: new Date().toISOString(),
            rows,
        };

        const json = JSON.stringify(rows);
        window.__IndyHubOptimizeDebugJson = json;

        console.groupCollapsed('[IndyHub] Optimize debug: buy/prod costs per item');
        console.log('productTypeId', productTypeId);
        console.log('unique items in demand', rows.length);
        console.log('Export JSON (fallback): window.__IndyHubOptimizeDebugJson');
        console.table(rows);
        console.groupEnd();

        // Try to put the full JSON into the clipboard automatically.
        // This avoids relying on DevTools-specific copy() helpers.
        try {
            if (navigator && navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
                await navigator.clipboard.writeText(json);
                if (window.CraftBP && typeof window.CraftBP.pushStatus === 'function') {
                    window.CraftBP.pushStatus(__('Optimizer debug JSON copied to clipboard'), 'success');
                }
            } else {
                // Fallback: prompt with the full JSON for manual copy.
                // eslint-disable-next-line no-alert
                window.prompt('Copy optimizer debug JSON:', json);
                if (window.CraftBP && typeof window.CraftBP.pushStatus === 'function') {
                    window.CraftBP.pushStatus(__('Optimizer debug JSON ready to copy (prompt opened)'), 'info');
                }
            }
        } catch (err) {
            // Clipboard can be blocked by browser permissions; fall back to prompt.
            // eslint-disable-next-line no-alert
            window.prompt('Copy optimizer debug JSON:', json);
            if (window.CraftBP && typeof window.CraftBP.pushStatus === 'function') {
                window.CraftBP.pushStatus(__('Clipboard blocked: debug JSON shown in prompt'), 'warning');
            }
        }
    } catch (e) {
        // Debug should never break optimization.
        console.warn('[IndyHub] Optimize debug failed', e);
    }

    // Apply decisions to switches.
    const applied = { buy: 0, prod: 0 };
    aggregateDecisions.forEach((mode, typeId) => {
        const switches = document.querySelectorAll(`input.mat-switch[data-type-id="${typeId}"]`);
        if (!switches || switches.length === 0) return;
        switches.forEach(switchEl => {
            if (switchEl.dataset.fixedMode === 'useless') return;
            switchEl.dataset.userState = mode;
            switchEl.checked = mode !== 'buy';
        });
        applied[mode] += 1;
    });

    refreshTreeSwitchHierarchy();
    if (window.SimulationAPI && typeof window.SimulationAPI.refreshFromDom === 'function') {
        window.SimulationAPI.refreshFromDom();
    }
    refreshTabsAfterStateChange({ forceNeeded: true });
    if (typeof recalcFinancials === 'function') {
        recalcFinancials();
    }

    if (window.CraftBP && typeof window.CraftBP.pushStatus === 'function') {
        const msg = __(`Optimized: ${applied.prod} prod, ${applied.buy} buy`);
        window.CraftBP.pushStatus(msg, lastChangeCount === 0 ? 'info' : 'success');
    }
}

// ==============================
// Run optimized tab (profitability vs runs)
// ==============================

function getPriceSnapshotFromSimulation(typeIds) {
    const state = (window.SimulationAPI && typeof window.SimulationAPI.getState === 'function')
        ? window.SimulationAPI.getState()
        : null;
    const prices = state && state.prices ? state.prices : null;
    const snapshot = new Map();

    if (prices instanceof Map && prices.size > 0) {
        prices.forEach((value, key) => {
            snapshot.set(Number(key), {
                fuzzwork: Number(value?.fuzzwork) || 0,
                real: Number(value?.real) || 0,
                sale: Number(value?.sale) || 0,
            });
        });
        return snapshot;
    }

    if (prices && typeof prices === 'object' && Object.keys(prices).length > 0) {
        Object.keys(prices).forEach((key) => {
            const id = Number(key);
            if (!id) return;
            const value = prices[key] || {};
            snapshot.set(id, {
                fuzzwork: Number(value?.fuzzwork) || 0,
                real: Number(value?.real) || 0,
                sale: Number(value?.sale) || 0,
            });
        });
        return snapshot;
    }

    // Fallback: some pages keep prices in SimulationAPI internals without exposing state.prices.
    // Build a minimal snapshot from getPrice() for the typeIds we care about.
    const ids = Array.isArray(typeIds) ? typeIds : [];
    const api = window.SimulationAPI;
    if (api && typeof api.getPrice === 'function') {
        ids.forEach((tid) => {
            const id = Number(tid);
            if (!Number.isFinite(id) || id <= 0) return;
            const buyInfo = api.getPrice(id, 'buy');
            const saleInfo = api.getPrice(id, 'sale');
            const buy = buyInfo && typeof buyInfo.value === 'number' ? buyInfo.value : 0;
            const sale = saleInfo && typeof saleInfo.value === 'number' ? saleInfo.value : 0;
            snapshot.set(id, {
                fuzzwork: Number(buy) || 0,
                real: Number(buy) || 0,
                sale: Number(sale) || 0,
            });
        });
    }

    return snapshot;
}

function collectTypeIdsFromMaterialsTree(nodes, out = new Set()) {
    (Array.isArray(nodes) ? nodes : []).forEach((node) => {
        const tid = Number(node?.type_id || node?.typeId) || 0;
        if (tid > 0) out.add(String(tid));
        const kids = node && (node.sub_materials || node.subMaterials);
        if (Array.isArray(kids) && kids.length) {
            collectTypeIdsFromMaterialsTree(kids, out);
        }
    });
    return out;
}

function getCurrentDecisionsFromDom() {
    const decisions = new Map();
    const treeTab = document.getElementById('tab-tree');
    if (!treeTab) return decisions;

    treeTab.querySelectorAll('input.mat-switch[data-type-id]').forEach((sw) => {
        const id = Number(sw.getAttribute('data-type-id')) || 0;
        if (!id) return;
        if (sw.dataset.fixedMode === 'useless' || sw.dataset.userState === 'useless') return;
        decisions.set(id, sw.checked ? 'prod' : 'buy');
    });
    return decisions;
}

function syncSimulationSwitchStatesFromDom() {
    const api = window.SimulationAPI;
    if (!api || typeof api.setSwitchState !== 'function') return;

    const treeTab = document.getElementById('tab-tree');
    if (!treeTab) return;

    treeTab.querySelectorAll('input.mat-switch[data-type-id]').forEach((sw) => {
        const id = Number(sw.getAttribute('data-type-id')) || 0;
        if (!id) return;
        if (sw.dataset.fixedMode === 'useless' || sw.dataset.userState === 'useless') {
            api.setSwitchState(id, 'useless');
            return;
        }
        api.setSwitchState(id, sw.checked ? 'prod' : 'buy');
    });
}

function getCurrentDecisionsFromSimulationOrDom() {
    const api = window.SimulationAPI;
    if (api && typeof api.getSwitchState === 'function') {
        const decisions = new Map();
        const treeTab = document.getElementById('tab-tree');
        if (!treeTab) return getCurrentDecisionsFromDom();

        treeTab.querySelectorAll('input.mat-switch[data-type-id]').forEach((sw) => {
            const id = Number(sw.getAttribute('data-type-id')) || 0;
            if (!id) return;
            const state = api.getSwitchState(id);
            if (state === 'buy' || state === 'prod' || state === 'useless') {
                decisions.set(id, state);
            }
        });

        // If SimulationAPI doesn't have any states yet, fall back to DOM.
        if (decisions.size > 0) return decisions;
    }

    return getCurrentDecisionsFromDom();
}

function generateRunScenarios(maxRuns) {
    const maxValue = Math.max(1, Number(maxRuns) || 1);

    // Evenly spaced scenarios:
    // - Target ~10 points across the range
    // - Examples: 100 -> step 10, 1000 -> step 100
    const targetPoints = 10;
    const step = Math.max(1, Math.round(maxValue / targetPoints));
    const runs = [1];
    for (let v = step; v < maxValue; v += step) {
        runs.push(v);
    }
    if (!runs.includes(maxValue)) {
        runs.push(maxValue);
    }

    return Array.from(new Set(runs))
        .map((v) => Math.max(1, Math.floor(Number(v) || 1)))
        .sort((a, b) => a - b);
}

function buildCraftPayloadUrlForRuns(testRuns) {
    let base = window.BLUEPRINT_DATA?.urls?.craft_bp_payload;
    let bpTypeId = Number(window.BLUEPRINT_DATA?.bp_type_id || window.BLUEPRINT_DATA?.bpTypeId || window.BLUEPRINT_DATA?.type_id || window.BLUEPRINT_DATA?.typeId || 0);

    // If the backend provided a craft_bp_payload URL, it is the most reliable source of the
    // blueprint type id; parse it so we don't depend on potentially mutated JS state.
    if (base) {
        const match = String(base).match(/\/craft-bp-payload\/(\d+)\//);
        if (match && match[1]) {
            const parsed = Number(match[1]);
            if (Number.isFinite(parsed) && parsed > 0) {
                bpTypeId = parsed;
            }
        }
    }

    // Fallback: construct endpoint if missing.
    if (!base && bpTypeId > 0) {
        base = `/indy_hub/api/craft-bp-payload/${bpTypeId}/`;
    }

    if (!base) {
        craftBPDebugLog('[RunOptimized] Missing craft_bp_payload base URL (urls.craft_bp_payload). bpTypeId=', bpTypeId);
        return null;
    }

    const url = new URL(base, window.location.origin);
    // IMPORTANT:
    // Run optimized must use the same ME/TE configuration that the server used to render
    // the current dashboard payload. Otherwise we can end up with a mismatch where
    // localStorage-restored per-blueprint ME/TE inputs (not yet applied) affect Run optimized
    // API payloads but not the dashboard totals.
    const currentParams = new URLSearchParams(window.location.search || '');

    const rootME = currentParams.has('me')
        ? Number(currentParams.get('me'))
        : (window.BLUEPRINT_DATA?.me ?? 0);
    const rootTE = currentParams.has('te')
        ? Number(currentParams.get('te'))
        : (window.BLUEPRINT_DATA?.te ?? 0);

    url.searchParams.set('runs', String(Math.max(1, Number(testRuns) || 1)));
    url.searchParams.set('me', String(rootME));
    url.searchParams.set('te', String(rootTE));

    // Propagate debug flag to backend so it can include _debug info in the JSON.
    if (window.INDY_HUB_DEBUG) {
        url.searchParams.set('indy_debug', '1');
    }

    // Only propagate per-blueprint overrides if they are already part of the page URL.
    // (Those are the overrides the backend actually applied to compute window.BLUEPRINT_DATA.)
    for (const [key, value] of currentParams.entries()) {
        if (key.startsWith('me_') || key.startsWith('te_')) {
            url.searchParams.set(key, String(value));
        }
    }

    const finalUrl = url.toString();
    craftBPDebugLog('[RunOptimized] craft_bp_payload URL built', finalUrl);
    return finalUrl;
}

async function fetchBlueprintPayloadForRuns(testRuns) {
    window.__indyHubRunOptimizedCache = window.__indyHubRunOptimizedCache || {};
    const cache = window.__indyHubRunOptimizedCache;
    const url = buildCraftPayloadUrlForRuns(testRuns);
    if (!url) {
        throw new Error('Missing craft_bp_payload URL');
    }

    // Cache by full URL (runs + ME/TE + blueprint configs + debug flag), not only by runs.
    const key = String(url);
    if (cache[key]) {
        return cache[key];
    }

    const response = await fetch(url, {
        headers: { 'Accept': 'application/json' },
        credentials: 'same-origin',
    });
    if (!response.ok) {
        throw new Error(`craft_bp_payload failed: ${response.status}`);
    }
    const json = await response.json();

    craftBPDebugLog('[RunOptimized] craft_bp_payload response', {
        requestUrl: url,
        responseUrl: response.url,
        type_id: json?.type_id,
        bp_type_id: json?.bp_type_id,
        product_type_id: json?.product_type_id,
        me: json?.me,
        te: json?.te,
        num_runs: json?.num_runs,
        final_product_qty: json?.final_product_qty,
        materials_tree_roots: Array.isArray(json?.materials_tree) ? json.materials_tree.length : null,
        recipe_map_keys: (json?.recipe_map && typeof json.recipe_map === 'object') ? Object.keys(json.recipe_map).length : null,
        _debug: json?._debug ?? null,
    });

    cache[key] = json;
    return json;
}

function computeOptimizedProfitabilityForPayload(payload, pricesSnapshot, options = {}) {
    const tree = Array.isArray(payload?.materials_tree) ? payload.materials_tree : [];
    const productTypeId = Number(payload?.product_type_id) || 0;
    const finalProductQty = Math.max(0, Math.ceil(Number(payload?.final_product_qty) || 0));

    // Prefer live SimulationAPI prices when available so Run optimized uses
    // the same buy/sale logic as the dashboard (real/fuzzwork overrides).
    const simulationApi = window.SimulationAPI;

    function getPriceRecord(typeId) {
        return pricesSnapshot.get(Number(typeId)) || { fuzzwork: 0, real: 0, sale: 0 };
    }

    function getBuyUnitPrice(typeId) {
        if (simulationApi && typeof simulationApi.getPrice === 'function') {
            const info = simulationApi.getPrice(typeId, 'buy');
            const v = info && typeof info.value === 'number' ? info.value : 0;
            if (v > 0) return v;
        }
        const record = getPriceRecord(typeId);
        const real = Number(record.real) || 0;
        if (real > 0) return real;
        const fuzz = Number(record.fuzzwork) || 0;
        if (fuzz > 0) return fuzz;
        return 0;
    }

    function getSellUnitPrice(typeId) {
        if (simulationApi && typeof simulationApi.getPrice === 'function') {
            const info = simulationApi.getPrice(typeId, 'sale');
            const v = info && typeof info.value === 'number' ? info.value : 0;
            if (v > 0) return v;
        }
        const record = getPriceRecord(typeId);
        const sale = Number(record.sale) || 0;
        if (sale > 0) return sale;
        const fuzz = Number(record.fuzzwork) || 0;
        if (fuzz > 0) return fuzz;
        const real = Number(record.real) || 0;
        if (real > 0) return real;
        return 0;
    }

    function getBuyUnitPriceOrInf(typeId) {
        const p = getBuyUnitPrice(typeId);
        return p > 0 ? p : Number.POSITIVE_INFINITY;
    }

    function readChildren(node) {
        const kids = node && (node.sub_materials || node.subMaterials);
        return Array.isArray(kids) ? kids : [];
    }

    function readTypeId(node) {
        return Number(node?.type_id || node?.typeId) || 0;
    }

    function readQty(node) {
        const q = Number(node?.quantity ?? node?.qty ?? 0);
        return Number.isFinite(q) ? Math.max(0, Math.ceil(q)) : 0;
    }

    function readProducedPerCycle(node) {
        const p = Number(node?.produced_per_cycle ?? node?.producedPerCycle ?? 0);
        return Number.isFinite(p) ? Math.max(0, Math.ceil(p)) : 0;
    }

    // Dashboard-aligned model: cost = bought items (leaf + craftables switched to BUY)
    // revenue = final product sale + surplus credit computed from pooled cycles per craftable type.
    function computeDisplayedMarginFromTreeTraversal(currentDecisions) {
        const leafNeeds = new Map();
        const buyCraftables = new Map();
        const prodCraftables = new Map();
        const producedPerCycleByType = new Map();

        function addToCounter(map, typeId, qty) {
            if (!typeId || qty <= 0) return;
            map.set(typeId, (map.get(typeId) || 0) + qty);
        }

        const walk = (nodes, blockedByBuyAncestor = false) => {
            (Array.isArray(nodes) ? nodes : []).forEach((node) => {
                if (blockedByBuyAncestor) return;
                const typeId = readTypeId(node);
                if (!typeId) return;

                const qty = readQty(node);
                const children = readChildren(node);
                const craftable = children.length > 0;

                const ppc = readProducedPerCycle(node);
                if (ppc > 0 && !producedPerCycleByType.has(typeId)) {
                    producedPerCycleByType.set(typeId, ppc);
                }

                if (craftable) {
                    const state = currentDecisions.get(typeId) || 'prod';
                    if (state === 'useless') return;
                    if (state === 'buy') {
                        addToCounter(buyCraftables, typeId, qty);
                        return;
                    }
                    addToCounter(prodCraftables, typeId, qty);
                    walk(children, false);
                    return;
                }

                addToCounter(leafNeeds, typeId, qty);
            });
        };

        walk(tree, false);

        let cost = 0;
        leafNeeds.forEach((qty, typeId) => {
            const unit = getBuyUnitPrice(typeId);
            if (unit > 0) cost += unit * qty;
        });
        buyCraftables.forEach((qty, typeId) => {
            const unit = getBuyUnitPrice(typeId);
            if (unit > 0) cost += unit * qty;
        });

        let surplusRevenue = 0;
        prodCraftables.forEach((totalNeeded, typeId) => {
            const ppc = producedPerCycleByType.get(typeId) || 0;
            if (!(ppc > 0) || !(totalNeeded > 0)) return;
            const cycles = Math.max(1, Math.ceil(totalNeeded / ppc));
            const totalProduced = cycles * ppc;
            const surplus = Math.max(0, totalProduced - totalNeeded);
            if (surplus <= 0) return;
            const unit = getSellUnitPrice(typeId);
            if (unit > 0) surplusRevenue += unit * surplus;
        });

        const productUnitSale = productTypeId ? getSellUnitPrice(productTypeId) : 0;
        const finalRev = (productUnitSale > 0 && finalProductQty > 0) ? (productUnitSale * finalProductQty) : 0;
        const revenue = finalRev + surplusRevenue;
        const profit = revenue - cost;
        const margin = revenue > 0 ? (profit / revenue) : Number.NEGATIVE_INFINITY;
        return { margin, profit, revenue, cost, surplusRevenue };
    }

    // If we were provided explicit decisions (e.g. current dashboard switches),
    // skip optimization and just compute the displayed margin for those decisions.
    if (options && options.decisions instanceof Map) {
        const snap = computeDisplayedMarginFromTreeTraversal(options.decisions);
        const marginPct = Number.isFinite(snap.margin) ? (snap.margin * 100) : 0;
        return {
            runs: Number(payload?.num_runs) || 1,
            cost: snap.cost,
            revenue: snap.revenue,
            profit: snap.profit,
            margin: marginPct,
        };
    }

    const occurrencesByType = new Map();
    const nameByType = new Map();
    (function collect(nodes) {
        (Array.isArray(nodes) ? nodes : []).forEach((node) => {
            const id = readTypeId(node);
            if (id) {
                const typeName = node?.type_name || node?.typeName || '';
                if (typeName && !nameByType.has(id)) nameByType.set(id, typeName);
            }
            const children = readChildren(node);
            if (id && children.length > 0) {
                if (!occurrencesByType.has(id)) occurrencesByType.set(id, []);
                occurrencesByType.get(id).push(node);
            }
            if (children.length > 0) {
                collect(children);
            }
        });
    })(tree);

    const recipes = new Map();
    const backendRecipeMap = payload?.recipe_map || payload?.recipeMap;
    if (backendRecipeMap && typeof backendRecipeMap === 'object' && Object.keys(backendRecipeMap).length > 0) {
        Object.entries(backendRecipeMap).forEach(([typeIdStr, recipe]) => {
            const typeId = Number(typeIdStr);
            if (!Number.isFinite(typeId) || !recipe) return;

            const producedPerCycle = Number(recipe?.produced_per_cycle ?? recipe?.producedPerCycle ?? 0);
            if (!Number.isFinite(producedPerCycle) || producedPerCycle <= 0) return;

            const inputsPerCycle = new Map();
            const inputs = recipe?.inputs_per_cycle ?? recipe?.inputsPerCycle ?? [];
            (Array.isArray(inputs) ? inputs : []).forEach((inp) => {
                const childTypeId = Number(inp?.type_id ?? inp?.typeId ?? 0);
                const perCycleQty = Number(inp?.quantity ?? inp?.qty ?? 0);
                if (!Number.isFinite(childTypeId) || childTypeId <= 0) return;
                if (!Number.isFinite(perCycleQty) || perCycleQty <= 0) return;
                inputsPerCycle.set(childTypeId, perCycleQty);
            });
            if (inputsPerCycle.size === 0) return;

            recipes.set(typeId, { producedPerCycle, inputsPerCycle });
        });
    } else {
        // Legacy fallback: infer a recipe from a single occurrence (less precise if a craftable appears multiple times).
        occurrencesByType.forEach((nodes, typeId) => {
            let best = null;
            let bestCycles = 0;
            nodes.forEach((n) => {
                const ppc = readProducedPerCycle(n);
                const needed = readQty(n);
                if (!ppc || !needed) return;
                const cycles = Math.max(1, Math.ceil(needed / ppc));
                if (cycles >= bestCycles) {
                    bestCycles = cycles;
                    best = n;
                }
            });
            if (!best) return;

            const ppc = readProducedPerCycle(best);
            const needed = readQty(best);
            if (!ppc || !needed) return;
            const cycles = Math.max(1, Math.ceil(needed / ppc));

            const inputsPerCycle = new Map();
            readChildren(best).forEach((child) => {
                const childTypeId = readTypeId(child);
                if (!childTypeId) return;
                const childQty = readQty(child);
                if (!childQty) return;
                inputsPerCycle.set(childTypeId, childQty / cycles);
            });
            recipes.set(typeId, { producedPerCycle: ppc, inputsPerCycle });
        });
    }

    const craftables = new Set(recipes.keys());
    const decisions = new Map();
    craftables.forEach((id) => decisions.set(id, 'prod'));

    function summarizeDecisions(decisionsMap) {
        const buy = [];
        const prod = [];

        if (!(decisionsMap instanceof Map)) {
            return { craftablesCount: 0, buyCount: 0, prodCount: 0, buy, prod };
        }

        decisionsMap.forEach((state, typeId) => {
            const id = Number(typeId) || 0;
            if (!id) return;
            const name = nameByType.get(id) || '';
            const entry = { typeId: id, typeName: name };
            if (state === 'buy') buy.push(entry);
            else prod.push(entry);
        });

        const byName = (a, b) => String(a.typeName || '').localeCompare(String(b.typeName || ''), undefined, { sensitivity: 'base' });
        buy.sort(byName);
        prod.sort(byName);

        return {
            craftablesCount: decisionsMap.size,
            buyCount: buy.length,
            prodCount: prod.length,
            buy,
            prod,
        };
    }

    const rootDemand = new Map();
    tree.forEach((rootNode) => {
        const id = readTypeId(rootNode);
        if (!id) return;
        const q = readQty(rootNode);
        if (!q) return;
        rootDemand.set(id, (rootDemand.get(id) || 0) + q);
    });

    const edges = new Map();
    const indegree = new Map();
    craftables.forEach((id) => {
        edges.set(id, new Set());
        indegree.set(id, 0);
    });
    recipes.forEach((rec, parentId) => {
        rec.inputsPerCycle.forEach((_, childId) => {
            if (!edges.has(parentId)) edges.set(parentId, new Set());
            edges.get(parentId).add(childId);
            if (craftables.has(childId)) {
                indegree.set(childId, (indegree.get(childId) || 0) + 1);
            }
        });
    });

    const queue = [];
    indegree.forEach((deg, id) => { if (deg === 0) queue.push(id); });
    const topo = [];
    while (queue.length) {
        const id = queue.shift();
        topo.push(id);
        (edges.get(id) || new Set()).forEach((childId) => {
            if (!craftables.has(childId)) return;
            const nextDeg = (indegree.get(childId) || 0) - 1;
            indegree.set(childId, nextDeg);
            if (nextDeg === 0) queue.push(childId);
        });
    }

    function computeDemand(currentDecisions) {
        const demand = new Map(rootDemand);
        topo.forEach((typeId) => {
            if (!craftables.has(typeId)) return;
            if ((currentDecisions.get(typeId) || 'prod') !== 'prod') return;
            const needed = demand.get(typeId) || 0;
            if (needed <= 0) return;
            const rec = recipes.get(typeId);
            if (!rec || !rec.producedPerCycle) return;
            const cycles = Math.max(1, Math.ceil(needed / rec.producedPerCycle));
            rec.inputsPerCycle.forEach((perCycleQty, childId) => {
                const add = Math.max(0, Math.ceil((perCycleQty * cycles) - 1e-9));
                if (add <= 0) return;
                demand.set(childId, (demand.get(childId) || 0) + add);
            });
        });
        return demand;
    }

    function computeBestUnitCosts(demand, currentDecisions) {
        const bestUnitCost = new Map();
        const chosenMode = new Map();
        const reverseTopo = topo.slice().reverse();

        reverseTopo.forEach((typeId) => {
            const needed = demand.get(typeId) || 0;
            if (!craftables.has(typeId) || needed <= 0) return;

            const buyUnit = getBuyUnitPriceOrInf(typeId);
            const buyTotal = buyUnit * needed;

            const rec = recipes.get(typeId);
            if (!rec || !rec.producedPerCycle) {
                bestUnitCost.set(typeId, buyUnit > 0 ? buyUnit : 0);
                chosenMode.set(typeId, 'buy');
                return;
            }

            const cycles = Math.max(1, Math.ceil(needed / rec.producedPerCycle));
            const produced = cycles * rec.producedPerCycle;
            const surplus = Math.max(0, produced - needed);

            let inputsCost = 0;
            rec.inputsPerCycle.forEach((perCycleQty, childId) => {
                const childQtyTotal = Math.max(0, Math.ceil((perCycleQty * cycles) - 1e-9));
                if (childQtyTotal <= 0) return;
                const childIsCraftable = craftables.has(childId);
                const childUnit = childIsCraftable
                    ? (bestUnitCost.get(childId) ?? getBuyUnitPriceOrInf(childId))
                    : getBuyUnitPriceOrInf(childId);
                inputsCost += childUnit * childQtyTotal;
            });

            const sellUnit = getSellUnitPrice(typeId);
            const credit = (sellUnit > 0 ? sellUnit : 0) * surplus;
            const prodTotal = inputsCost - credit;
            const prodUnit = needed > 0 ? (prodTotal / needed) : Number.POSITIVE_INFINITY;

            let mode;
            if (!Number.isFinite(prodTotal) && !Number.isFinite(buyTotal)) {
                mode = currentDecisions.get(typeId) || 'prod';
            } else {
                mode = (prodTotal <= buyTotal) ? 'prod' : 'buy';
            }
            chosenMode.set(typeId, mode);
            bestUnitCost.set(typeId, mode === 'prod' ? prodUnit : buyUnit);
        });

        craftables.forEach((typeId) => {
            if (!chosenMode.has(typeId)) {
                chosenMode.set(typeId, currentDecisions.get(typeId) || 'prod');
            }
        });

        return { chosenMode };
    }

    function stabilizeBottomUp(decisionsMap) {
        let totalChanged = 0;
        for (let iter = 0; iter < 6; iter += 1) {
            const demand = computeDemand(decisionsMap);
            const { chosenMode } = computeBestUnitCosts(demand, decisionsMap);
            let changed = 0;
            chosenMode.forEach((mode, typeId) => {
                const prev = decisionsMap.get(typeId) || 'prod';
                if (prev !== mode) {
                    decisionsMap.set(typeId, mode);
                    changed += 1;
                }
            });
            totalChanged += changed;
            if (changed === 0) break;
        }
        return totalChanged;
    }

    // Helper: compute the "displayed" margin (as in the KPI dashboard) for a given decision set
    function computeDisplayedMargin(currentDecisions) {
        const demand = computeDemand(currentDecisions);

        let cost = 0;
        demand.forEach((qty, typeId) => {
            const id = Number(typeId) || 0;
            if (!id) return;

            const isCraftable = craftables.has(id);
            if (isCraftable) {
                if ((currentDecisions.get(id) || 'prod') !== 'buy') {
                    return;
                }
            }

            const unit = getBuyUnitPrice(id);
            if (unit > 0) {
                cost += unit * qty;
            }
        });

        let surplusRev = 0;
        craftables.forEach((typeId) => {
            if ((currentDecisions.get(typeId) || 'prod') !== 'prod') return;
            const needed = demand.get(typeId) || 0;
            if (needed <= 0) return;
            const rec = recipes.get(typeId);
            if (!rec || !rec.producedPerCycle) return;
            const cycles = Math.max(1, Math.ceil(needed / rec.producedPerCycle));
            const produced = cycles * rec.producedPerCycle;
            const surplus = Math.max(0, produced - needed);
            if (surplus <= 0) return;
            const unit = getSellUnitPrice(typeId);
            if (unit > 0) surplusRev += unit * surplus;
        });

        const productUnitSale = productTypeId ? getSellUnitPrice(productTypeId) : 0;
        const finalRev = (productUnitSale > 0 && finalProductQty > 0) ? (productUnitSale * finalProductQty) : 0;
        const revenue = finalRev + surplusRev;
        const profit = revenue - cost;
        const margin = revenue > 0 ? (profit / revenue) : Number.NEGATIVE_INFINITY;

        return { margin, profit, revenue, cost, surplusRevenue: surplusRev };
    }

    // Margin-first refinement: greedily flip switches (more iterations) to maximize displayed margin
    function greedyImproveMargin(decisionsMap, startingSnap) {
        const candidates = Array.from(craftables.keys());
        let best = startingSnap;
        const epsilon = 1e-9;
        let improved = false;

        for (let iter = 0; iter < 10; iter += 1) {
            let bestType = null;
            let bestNewState = null;
            let bestNew = null;
            let bestGain = 0;

            candidates.forEach((typeId) => {
                const current = decisionsMap.get(typeId) || 'prod';
                if (current !== 'buy' && current !== 'prod') return;
                const trial = current === 'buy' ? 'prod' : 'buy';

                decisionsMap.set(typeId, trial);
                const snap = computeDisplayedMargin(decisionsMap);
                const gain = snap.margin - best.margin;
                decisionsMap.set(typeId, current);

                if (gain > bestGain + epsilon) {
                    bestGain = gain;
                    bestType = typeId;
                    bestNewState = trial;
                    bestNew = snap;
                }
            });

            if (!bestType || !bestNewState || !bestNew || bestGain <= epsilon) {
                break;
            }

            decisionsMap.set(bestType, bestNewState);
            best = bestNew;
            improved = true;
        }

        return { best, improved };
    }

    // Multi-pass: bottom-up then greedy, until stable or max passes
    let bestSnapshot = computeDisplayedMargin(decisions);
    let bestDecisions = new Map(decisions);
    for (let pass = 0; pass < 5; pass += 1) {
        const changedBottomUp = stabilizeBottomUp(decisions);
        const snapAfterBottomUp = computeDisplayedMargin(decisions);
        const { best, improved } = greedyImproveMargin(decisions, snapAfterBottomUp);
        bestSnapshot = best;
        bestDecisions = new Map(decisions);
        if (changedBottomUp === 0 && !improved) {
            break;
        }
    }

    // Use the best margin-first result after passes
    const marginPct = Number.isFinite(bestSnapshot.margin) ? (bestSnapshot.margin * 100) : 0;
    return {
        runs: Number(payload?.num_runs) || 1,
        cost: bestSnapshot.cost,
        revenue: bestSnapshot.revenue,
        profit: bestSnapshot.profit,
        margin: marginPct, // Convert to percentage (clamped for display)
        config: summarizeDecisions(bestDecisions),
    };
}

function renderRunOptimizedChart(canvas, points) {
    if (!canvas || !canvas.getContext || !Array.isArray(points) || points.length === 0) {
        return;
    }

    const normalizedPoints = points
        .map((p) => {
            const runs = Math.max(1, Number(p?.runs) || 1);
            const rawMargin = Number(p?.margin);
            const margin = Number.isFinite(rawMargin) ? rawMargin : 0;
            return { runs, margin };
        })
        .sort((a, b) => a.runs - b.runs);

    const dpr = window.devicePixelRatio || 1;
    const cssWidth = canvas.clientWidth || 600;
    const cssHeight = canvas.getAttribute('height') ? Number(canvas.getAttribute('height')) : 240;
    canvas.width = Math.floor(cssWidth * dpr);
    canvas.height = Math.floor(cssHeight * dpr);
    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    const padding = 40;
    const w = cssWidth;
    const h = cssHeight;

    const xs = normalizedPoints.map((p) => p.runs);
    const ys = normalizedPoints.map((p) => p.margin);
    const minX = Math.min.apply(null, xs);
    const maxX = Math.max.apply(null, xs);

    // Force margin axis to 0–100% for consistent readability.
    // (Negative or >100% margins will be clipped to the chart bounds.)
    const minY = 0;
    const maxY = 100;

    function xToPx(x) {
        const safeX = Math.max(1, Number(x) || 1);
        const t = (maxX - minX) > 0 ? ((safeX - minX) / (maxX - minX)) : 0.5;
        return padding + t * (w - padding * 2);
    }

    function yToPx(y) {
        const safeY = Math.max(minY, Math.min(maxY, Number(y) || 0));
        const t = (maxY - minY) > 0 ? ((safeY - minY) / (maxY - minY)) : 0.5;
        return (h - padding) - t * (h - padding * 2);
    }

    // Clear
    ctx.clearRect(0, 0, w, h);

    // Axes
    ctx.strokeStyle = 'rgba(0,0,0,0.25)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(padding, padding);
    ctx.lineTo(padding, h - padding);
    ctx.lineTo(w - padding, h - padding);
    ctx.stroke();

    // Y ticks
    ctx.fillStyle = 'rgba(0,0,0,0.7)';
    ctx.font = '12px system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif';
    const yTicks = 4;
    for (let i = 0; i <= yTicks; i += 1) {
        const v = minY + (i / yTicks) * (maxY - minY);
        const y = yToPx(v);
        ctx.strokeStyle = 'rgba(0,0,0,0.08)';
        ctx.beginPath();
        ctx.moveTo(padding, y);
        ctx.lineTo(w - padding, y);
        ctx.stroke();
        ctx.fillText(`${v.toFixed(1)}%`, 6, y + 4);
    }

    // X ticks (evenly spaced)
    const xTickSet = new Set();
    xTickSet.add(minX);
    xTickSet.add(maxX);

    const targetXTicks = 10;
    const xStep = Math.max(1, Math.round(maxX / targetXTicks));
    for (let v = xStep; v < maxX; v += xStep) {
        if (v >= minX) {
            xTickSet.add(v);
        }
    }
    // Always label scenario points so users see what was computed.
    normalizedPoints.forEach((p) => xTickSet.add(p.runs));

    const xTicks = Array.from(xTickSet).sort((a, b) => a - b);
    ctx.font = '11px system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif';
    ctx.fillStyle = 'rgba(0,0,0,0.7)';
    let lastLabelX = -Infinity;
    xTicks.forEach((v) => {
        const x = xToPx(v);

        // Vertical grid line
        ctx.strokeStyle = 'rgba(0,0,0,0.06)';
        ctx.beginPath();
        ctx.moveTo(x, padding);
        ctx.lineTo(x, h - padding);
        ctx.stroke();

        // Tick mark
        ctx.strokeStyle = 'rgba(0,0,0,0.18)';
        ctx.beginPath();
        ctx.moveTo(x, h - padding);
        ctx.lineTo(x, h - padding + 5);
        ctx.stroke();

        // Label (skip if too close to previous to avoid clutter)
        if ((x - lastLabelX) >= 28 || v === minX || v === maxX) {
            const label = String(v);
            const tw = ctx.measureText(label).width;
            ctx.fillText(label, x - (tw / 2), h - 12);
            lastLabelX = x;
        }
    });

    // Line
    ctx.strokeStyle = '#0d6efd';
    ctx.lineWidth = 2;
    ctx.beginPath();
    normalizedPoints.forEach((p, idx) => {
        const x = xToPx(p.runs);
        const y = yToPx(p.margin);
        if (idx === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
    });
    ctx.stroke();

    // Points
    ctx.fillStyle = '#0d6efd';
    normalizedPoints.forEach((p) => {
        const x = xToPx(p.runs);
        const y = yToPx(p.margin);
        ctx.beginPath();
        ctx.arc(x, y, 3, 0, Math.PI * 2);
        ctx.fill();
    });

    // Margin labels per computed point
    ctx.font = '10px system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif';
    ctx.textBaseline = 'middle';
    normalizedPoints.forEach((p) => {
        const x = xToPx(p.runs);
        const y = yToPx(p.margin);
        const label = `${Number(p.margin).toFixed(1)}%`;
        // Simple outline for readability
        ctx.strokeStyle = 'rgba(255,255,255,0.9)';
        ctx.lineWidth = 3;
        ctx.strokeText(label, x + 6, y - 10);
        ctx.fillStyle = 'rgba(13,110,253,0.95)';
        ctx.fillText(label, x + 6, y - 10);
        ctx.fillStyle = '#0d6efd';
    });

    // X axis label
    ctx.fillStyle = 'rgba(0,0,0,0.7)';
    ctx.font = '12px system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif';
    ctx.fillText('runs', padding, padding - 10);
}

function pickBestRunPoint(points) {
    if (!Array.isArray(points) || points.length === 0) return null;
    let best = null;
    points.forEach((p) => {
        const runs = Number(p?.runs) || 0;
        const margin = Number(p?.margin);
        const revenue = Number(p?.revenue);
        const profit = Number(p?.profit);
        if (!(runs > 0)) return;
        if (!Number.isFinite(margin)) return;
        if (!Number.isFinite(revenue) || revenue <= 0) return;
        if (!best) {
            best = p;
            return;
        }
        const bestMargin = Number(best?.margin);
        if (margin > bestMargin + 1e-9) {
            best = p;
            return;
        }
        // Tie-break: prefer higher profit if margins are equal-ish.
        if (Math.abs(margin - bestMargin) <= 1e-6) {
            const bestProfit = Number(best?.profit);
            if (Number.isFinite(profit) && Number.isFinite(bestProfit) && profit > bestProfit) {
                best = p;
            }
        }
    });
    return best;
}

function buildRunSearchCandidates(maxRuns) {
    const maxValue = Math.max(1, Number(maxRuns) || 1);
    const set = new Set();

    const addRange = (start, end, step) => {
        const s = Math.max(1, Math.floor(Number(start) || 1));
        const e = Math.max(1, Math.floor(Number(end) || 1));
        const st = Math.max(1, Math.floor(Number(step) || 1));
        for (let r = s; r <= e; r += st) {
            set.add(r);
        }
    };

    // Dense sampling for small run counts (rounding effects are strongest here).
    if (maxValue <= 250) {
        addRange(1, maxValue, 1);
        return Array.from(set).sort((a, b) => a - b);
    }

    addRange(1, Math.min(50, maxValue), 1);
    addRange(60, Math.min(500, maxValue), 10);
    addRange(600, Math.min(2000, maxValue), 50);

    if (maxValue > 2000) {
        const step = Math.max(100, Math.round(maxValue / 60));
        addRange(2500, maxValue, step);
    }

    set.add(maxValue);
    return Array.from(set).sort((a, b) => a - b);
}

function renderBestRunSummary(bestEl, bestPoint, label) {
    if (!bestEl) return;
    if (!bestPoint) {
        bestEl.textContent = '';
        return;
    }
    const runs = Number(bestPoint?.runs) || 0;
    const margin = Number(bestPoint?.margin);
    const profit = Number(bestPoint?.profit);
    const revenue = Number(bestPoint?.revenue);

    if (!(runs > 0) || !Number.isFinite(margin)) {
        bestEl.textContent = '';
        return;
    }

    const parts = [];
    if (label) parts.push(`<span class="text-muted">${label}:</span>`);
    parts.push(`<span class="badge text-bg-primary">${__('Best')} ${margin.toFixed(1)}% @ ${runs} runs</span>`);
    if (Number.isFinite(profit) && Number.isFinite(revenue) && revenue > 0) {
        parts.push(`<span class="text-muted">${__('Profit')} ${formatPrice(profit)}</span>`);
    }
    bestEl.innerHTML = parts.join(' ');
}

function renderBestRunConfigDetails(bestPoint) {
    const detailsEl = document.getElementById('runOptimizedBestConfigDetails');
    const preEl = document.getElementById('runOptimizedBestConfigPre');
    const hintEl = document.getElementById('runOptimizedBestConfigHint');
    const copyBtn = document.getElementById('runOptimizedCopyBestConfig');

    if (!detailsEl || !preEl) return;

    const cfg = bestPoint && bestPoint.config;
    if (!cfg || typeof cfg !== 'object' || !Array.isArray(cfg.buy)) {
        detailsEl.style.display = 'none';
        preEl.textContent = '';
        if (hintEl) hintEl.textContent = '';
        return;
    }

    const runs = Number(bestPoint?.runs) || 0;
    const margin = Number(bestPoint?.margin);

    const lines = [];
    lines.push(`runs: ${runs}`);
    if (Number.isFinite(margin)) lines.push(`margin_pct: ${margin.toFixed(4)}`);
    lines.push(`craftables_total: ${Number(cfg.craftablesCount) || 0}`);
    lines.push(`buy_count: ${Number(cfg.buyCount) || 0}`);
    lines.push(`prod_count: ${Number(cfg.prodCount) || 0}`);
    lines.push('');
    lines.push('BUY:');
    cfg.buy.forEach((e) => {
        const id = Number(e?.typeId) || 0;
        const name = String(e?.typeName || '').trim();
        lines.push(`- ${id}${name ? `  ${name}` : ''}`);
    });

    preEl.textContent = lines.join('\n');
    detailsEl.style.display = '';
    if (hintEl) {
        hintEl.textContent = __('This is the best per-run optimized Buy/Prod configuration for the selected runs value.');
    }

    if (copyBtn && !copyBtn.__indyHubBound) {
        copyBtn.__indyHubBound = true;
        copyBtn.addEventListener('click', async () => {
            const text = preEl.textContent || '';
            try {
                if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
                    await navigator.clipboard.writeText(text);
                    return;
                }
            } catch (e) {
                // ignore
            }
            try {
                const ta = document.createElement('textarea');
                ta.value = text;
                document.body.appendChild(ta);
                ta.select();
                document.execCommand('copy');
                ta.remove();
            } catch (e) {
                // ignore
            }
        });
    }
}

async function findOptimalRuns({
    searchMaxRuns,
    pricesSnapshot,
    mode,
    decisions,
    statusEl,
}) {
    const maxValue = Math.max(1, Math.floor(Number(searchMaxRuns) || 1));
    const candidates = buildRunSearchCandidates(maxValue);
    const results = [];

    const computeForPayload = (payload) => {
        if (mode === 'dashboard' && decisions instanceof Map) {
            return computeOptimizedProfitabilityForPayload(payload, pricesSnapshot, { decisions });
        }
        return computeOptimizedProfitabilityForPayload(payload, pricesSnapshot);
    };

    for (let i = 0; i < candidates.length; i += 1) {
        const runs = candidates[i];
        if (statusEl) {
            statusEl.textContent = __(`Searching best runs: ${i + 1}/${candidates.length} (runs=${runs})…`);
        }
        const payload = await fetchBlueprintPayloadForRuns(runs);
        const snap = computeForPayload(payload);
        results.push(snap);
    }

    // Local refinement around the best candidate (±50 runs) when the range is large.
    const bestCoarse = pickBestRunPoint(results);
    if (bestCoarse && maxValue > 250) {
        const center = Math.max(1, Math.floor(Number(bestCoarse.runs) || 1));
        const start = Math.max(1, center - 50);
        const end = Math.min(maxValue, center + 50);
        const refineRuns = [];
        for (let r = start; r <= end; r += 1) refineRuns.push(r);

        for (let i = 0; i < refineRuns.length; i += 1) {
            const runs = refineRuns[i];
            if (statusEl) {
                statusEl.textContent = __(`Refining: ${i + 1}/${refineRuns.length} (runs=${runs})…`);
            }
            const payload = await fetchBlueprintPayloadForRuns(runs);
            const snap = computeForPayload(payload);
            results.push(snap);
        }
    }

    return { best: pickBestRunPoint(results), samples: results.length };
}

function initializeRunOptimizedTab() {
    const tabBtn = document.getElementById('run-optimized-tab-btn');
    if (!tabBtn) return;

    let inFlight = false;
    let initialized = false;

    // Persist state across tab shows so click handlers can reuse it.
    const state = {
        pricesSnapshot: null,
        ids: null,
        decisions: null,
        mode: 'dashboard',
        ui: {},
        computeAndRenderCurve: null,
    };
    tabBtn.addEventListener('shown.bs.tab', async function () {

        const modeToggleEl = document.getElementById('runOptimizedUseDashboardDecisions');
        const searchMaxRunsEl = document.getElementById('runOptimizedSearchMaxRuns');
        const findBestBtn = document.getElementById('runOptimizedFindBestBtn');
        const bestResultEl = document.getElementById('runOptimizedBestResult');

        state.ui = { modeToggleEl, searchMaxRunsEl, findBestBtn, bestResultEl };

        function getRunOptimizedMode() {
            return (modeToggleEl && modeToggleEl.checked) ? 'dashboard' : 'optimize';
        }

        async function computeAndRenderCurve() {
            if (inFlight) return;
            inFlight = true;

            const statusEl = document.getElementById('run-optimized-status');
            const canvas = document.getElementById('runOptimizedChart');

            craftBPDebugLog('[RunOptimized] Tab shown');
            craftBPDebugLog('[RunOptimized] URL', window.location && window.location.href);
            craftBPDebugLog('[RunOptimized] SimulationAPI available?', Boolean(window.SimulationAPI));
            craftBPDebugLog('[RunOptimized] SimulationAPI methods', {
                getPrice: typeof window.SimulationAPI?.getPrice,
                setPrice: typeof window.SimulationAPI?.setPrice,
                refreshFromDom: typeof window.SimulationAPI?.refreshFromDom,
                getState: typeof window.SimulationAPI?.getState,
                getFinancialItems: typeof window.SimulationAPI?.getFinancialItems,
            });

            const mode = getRunOptimizedMode();
            state.mode = mode;

            try {
                if (statusEl) {
                    statusEl.className = 'alert alert-info mb-3';
                    statusEl.textContent = __('Loading prices and computing profitability curve…');
                }

                // Ensure we have fuzzwork buy prices available.
                const currentTree = window.BLUEPRINT_DATA?.materials_tree;
                const productTypeId = Number(window.BLUEPRINT_DATA?.product_type_id || window.BLUEPRINT_DATA?.productTypeId || CRAFT_BP?.productTypeId) || 0;

                const ids = Array.from(collectTypeIdsFromMaterialsTree(currentTree || []));
                if (productTypeId) ids.push(String(productTypeId));
                state.ids = ids;

                craftBPDebugLog('[RunOptimized] productTypeId', productTypeId);
                craftBPDebugLog('[RunOptimized] IDs count', ids.length);
                craftBPDebugLog('[RunOptimized] IDs sample (first 25)', ids.slice(0, 25));
                craftBPDebugLog('[RunOptimized] fuzzworkUrl (CRAFT_BP)', CRAFT_BP.fuzzworkUrl);
                craftBPDebugLog('[RunOptimized] fuzzworkUrl (BLUEPRINT_DATA)', window.BLUEPRINT_DATA?.urls?.fuzzwork_price || window.BLUEPRINT_DATA?.fuzzwork_price_url);

                if (typeof fetchAllPrices === 'function' && ids.length > 0 && window.SimulationAPI && typeof window.SimulationAPI.setPrice === 'function') {
                    craftBPDebugLog('[RunOptimized] Fetching Fuzzwork prices…');
                    const prices = await fetchAllPrices(ids);

                    const priceKeys = prices && typeof prices === 'object' ? Object.keys(prices) : [];
                    craftBPDebugLog('[RunOptimized] Fuzzwork prices keys', priceKeys.length);
                    if (productTypeId) {
                        const k = String(productTypeId);
                        craftBPDebugLog('[RunOptimized] Fuzzwork product raw price', prices[k] ?? prices[String(parseInt(k, 10))]);
                    }

                    let missingCount = 0;
                    let zeroCount = 0;
                    ids.forEach((tid) => {
                        const raw = prices[tid] ?? prices[String(parseInt(tid, 10))];
                        if (raw === undefined || raw === null) {
                            missingCount += 1;
                            return;
                        }
                        const p = parseFloat(raw);
                        if (!(p > 0)) {
                            zeroCount += 1;
                        }
                    });
                    craftBPDebugLog('[RunOptimized] Fuzzwork missing count', missingCount, 'zero/non-positive count', zeroCount);

                    ids.forEach((tid) => {
                        const raw = prices[tid] ?? prices[String(parseInt(tid, 10))];
                        const price = raw != null ? (parseFloat(raw) || 0) : 0;
                        if (price > 0) {
                            window.SimulationAPI.setPrice(tid, 'fuzzwork', price);
                        }
                    });

                    // Ensure final product has a sell price fallback when not explicitly set.
                    if (productTypeId) {
                        const finalKey = String(productTypeId);
                        const rawFinal = prices[finalKey] ?? prices[String(parseInt(finalKey, 10))];
                        const finalPrice = rawFinal != null ? (parseFloat(rawFinal) || 0) : 0;
                        if (finalPrice > 0 && typeof window.SimulationAPI.getPrice === 'function') {
                            const existingSale = window.SimulationAPI.getPrice(productTypeId, 'sale');
                            const existingSaleValue = existingSale && typeof existingSale.value === 'number' ? existingSale.value : 0;
                            if (!(existingSaleValue > 0)) {
                                window.SimulationAPI.setPrice(productTypeId, 'sale', finalPrice);
                            }
                        }
                    }
                }

                const pricesSnapshot = getPriceSnapshotFromSimulation(ids);
                state.pricesSnapshot = pricesSnapshot;

                craftBPDebugLog('[RunOptimized] Snapshot size', pricesSnapshot instanceof Map ? pricesSnapshot.size : null);
                if (productTypeId) {
                    craftBPDebugLog('[RunOptimized] Snapshot product record', pricesSnapshot.get(Number(productTypeId)));
                    if (window.SimulationAPI && typeof window.SimulationAPI.getPrice === 'function') {
                        craftBPDebugLog('[RunOptimized] SimulationAPI product buy/sale', {
                            buy: window.SimulationAPI.getPrice(productTypeId, 'buy'),
                            sale: window.SimulationAPI.getPrice(productTypeId, 'sale'),
                        });
                    }
                }

                const maxRuns = Number(document.getElementById('runsInput')?.value || window.BLUEPRINT_DATA?.num_runs || 1);
                const scenarios = generateRunScenarios(maxRuns);

                craftBPDebugLog('[RunOptimized] maxRuns', maxRuns);
                craftBPDebugLog('[RunOptimized] scenarios', scenarios);

                // Default search upper bound.
                if (searchMaxRunsEl && !String(searchMaxRunsEl.value || '').trim()) {
                    const suggested = Math.min(10000, Math.max(maxRuns, maxRuns * 10));
                    searchMaxRunsEl.value = String(suggested);
                }

                // Keep SimulationAPI switch state aligned with the DOM without touching prices.
                syncSimulationSwitchStatesFromDom();
                const decisions = getCurrentDecisionsFromSimulationOrDom();
                state.decisions = decisions;

                const results = [];
                for (let i = 0; i < scenarios.length; i += 1) {
                    const runs = scenarios[i];
                    if (statusEl) {
                        statusEl.textContent = __(`Computing ${i + 1}/${scenarios.length} (runs=${runs})…`);
                    }

                    const payload = await fetchBlueprintPayloadForRuns(runs);
                    craftBPDebugLog('[RunOptimized] payload received', {
                        type_id: payload?.type_id,
                        bp_type_id: payload?.bp_type_id,
                        runs: payload?.num_runs,
                        product_type_id: payload?.product_type_id,
                        final_product_qty: payload?.final_product_qty,
                        recipe_map_keys: payload?.recipe_map ? Object.keys(payload.recipe_map).length : 0,
                        materials_tree_roots: Array.isArray(payload?.materials_tree) ? payload.materials_tree.length : 0,
                    });

                    const snap = (mode === 'dashboard')
                        ? computeOptimizedProfitabilityForPayload(payload, pricesSnapshot, { decisions })
                        : computeOptimizedProfitabilityForPayload(payload, pricesSnapshot);
                    results.push(snap);

                    craftBPDebugLog('[RunOptimized] scenario result', {
                        runs: snap?.runs,
                        cost: snap?.cost,
                        revenue: snap?.revenue,
                        profit: snap?.profit,
                        marginPct: snap?.margin,
                    });
                }

                // Debug: compare dashboard margin vs maxRuns point margin only in dashboard-aligned mode.
                if (mode === 'dashboard' && window.INDY_HUB_DEBUG && window.SimulationAPI && typeof window.SimulationAPI.getFinancialItems === 'function') {
                    try {
                        const api = window.SimulationAPI;
                        const productTypeIdDbg = Number(window.BLUEPRINT_DATA?.product_type_id || window.BLUEPRINT_DATA?.productTypeId || CRAFT_BP?.productTypeId) || 0;

                        let costTotal = 0;
                        const items = api.getFinancialItems() || [];
                        items.forEach((item) => {
                            const typeId = Number(item.typeId ?? item.type_id) || 0;
                            if (!typeId || (productTypeIdDbg && typeId === productTypeIdDbg)) return;
                            const qty = Math.max(0, Math.ceil(Number(item.quantity ?? item.qty ?? 0))) || 0;
                            if (!qty) return;
                            const unit = api.getPrice(typeId, 'buy');
                            const unitPrice = unit && typeof unit.value === 'number' ? unit.value : 0;
                            if (unitPrice > 0) costTotal += unitPrice * qty;
                        });

                        let revenueTotal = 0;
                        try {
                            const finalRow = document.getElementById('finalProductRow');
                            const finalQtyEl = finalRow ? finalRow.querySelector('[data-qty]') : null;
                            const rawFinalQty = finalQtyEl ? (finalQtyEl.getAttribute('data-qty') || finalQtyEl.dataset?.qty) : null;
                            const finalQty = Math.max(0, Math.ceil(Number(rawFinalQty))) || 0;
                            if (productTypeIdDbg && finalQty > 0) {
                                const unit = api.getPrice(productTypeIdDbg, 'sale');
                                const unitPrice = unit && typeof unit.value === 'number' ? unit.value : 0;
                                if (unitPrice > 0) revenueTotal += unitPrice * finalQty;
                            }
                        } catch (e) {
                            // ignore
                        }

                        let surplusRevenue = 0;
                        const cycles = (typeof api.getProductionCycles === 'function') ? (api.getProductionCycles() || []) : [];
                        if (Array.isArray(cycles) && cycles.length) {
                            cycles.forEach((entry) => {
                                const typeId = Number(entry.typeId || entry.type_id || 0) || 0;
                                const surplusQty = Number(entry.surplus) || 0;
                                if (!typeId || surplusQty <= 0) return;
                                if (productTypeIdDbg && typeId === productTypeIdDbg) return;
                                const unit = api.getPrice(typeId, 'sale');
                                const unitPrice = unit && typeof unit.value === 'number' ? unit.value : 0;
                                if (unitPrice > 0) surplusRevenue += unitPrice * surplusQty;
                            });
                        }
                        revenueTotal += surplusRevenue;

                        const dashboardMargin = revenueTotal > 0 ? ((revenueTotal - costTotal) / revenueTotal) * 100 : 0;
                        const maxRunsPoint = results.find((r) => Number(r?.runs) === Number(maxRuns));

                        const domSummaryMargin = document.getElementById('financialSummaryMargin')?.textContent || null;
                        const domQuickMargin = document.getElementById('quickMargin')?.textContent || null;
                        const domHeroMargin = document.getElementById('heroMargin')?.textContent || null;
                        craftBPDebugLog('[RunOptimized] dashboard vs maxRuns point', {
                            maxRuns,
                            dashboard: { marginPct: dashboardMargin, revenue: revenueTotal, cost: costTotal, surplusRevenue },
                            point: maxRunsPoint || null,
                            dom: { financialSummaryMargin: domSummaryMargin, quickMargin: domQuickMargin, heroMargin: domHeroMargin },
                        });

                        try {
                            // eslint-disable-next-line no-console
                            console.log('[RunOptimized] dashboard vs maxRuns point JSON', JSON.stringify({
                                maxRuns,
                                dashboard: { marginPct: dashboardMargin, revenue: revenueTotal, cost: costTotal, surplusRevenue },
                                point: maxRunsPoint || null,
                                dom: { financialSummaryMargin: domSummaryMargin, quickMargin: domQuickMargin, heroMargin: domHeroMargin },
                            }));
                        } catch (e) {
                            // ignore
                        }
                    } catch (e) {
                        craftBPDebugLog('[RunOptimized] dashboard compare failed', e);
                    }
                }

                renderRunOptimizedChart(canvas, results);

                // Render a compact list of computed points (runs -> margin %)
                const pointsListEl = document.getElementById('runOptimizedPointsList');
                if (pointsListEl) {
                    const items = results
                        .slice()
                        .sort((a, b) => (Number(a?.runs) || 0) - (Number(b?.runs) || 0))
                        .map((r) => {
                            const runs = Number(r?.runs) || 0;
                            const margin = Number.isFinite(Number(r?.margin)) ? Number(r.margin) : 0;
                            return `<span class="badge text-bg-light border me-1 mb-1">${runs}: ${margin.toFixed(1)}%</span>`;
                        })
                        .join('');

                    pointsListEl.innerHTML = items || '';
                }

                // Show best run within displayed points.
                renderBestRunSummary(bestResultEl, pickBestRunPoint(results), __('Best within chart'));

                // Hint if flat 0.
                const productTypeIdForHint = Number(window.BLUEPRINT_DATA?.product_type_id || window.BLUEPRINT_DATA?.productTypeId || CRAFT_BP?.productTypeId) || 0;
                const api = window.SimulationAPI;
                const unitSale = (api && typeof api.getPrice === 'function') ? api.getPrice(productTypeIdForHint, 'sale') : null;
                const unitSaleValue = unitSale && typeof unitSale.value === 'number' ? unitSale.value : 0;
                const anyNonZero = results.some(r => Number(r?.margin) !== 0);

                if (statusEl) {
                    if (!anyNonZero && productTypeIdForHint && unitSaleValue <= 0) {
                        statusEl.className = 'alert alert-warning mb-3';
                        statusEl.textContent = __('Profitability curve is flat because the final product sell price is 0. Set a Sale price (or load prices) and retry.');
                    } else {
                        statusEl.className = 'alert alert-success mb-3';
                        statusEl.textContent = (mode === 'dashboard')
                            ? __('Profitability curve computed (dashboard-aligned).')
                            : __('Profitability curve computed (re-optimized per run).');
                    }
                }
            } catch (e) {
                console.error('[IndyHub] Run optimized failed', e);
                const statusEl = document.getElementById('run-optimized-status');
                if (statusEl) {
                    statusEl.className = 'alert alert-warning mb-3';
                    statusEl.textContent = __('Failed to compute profitability curve.');
                }
            } finally {
                inFlight = false;
            }
        }

        // Keep a reference to the latest compute function so one-time handlers can call it.
        state.computeAndRenderCurve = computeAndRenderCurve;

        if (!initialized) {
            // Recompute curve when the mode toggle changes.
            if (modeToggleEl) {
                modeToggleEl.addEventListener('change', () => {
                    state.computeAndRenderCurve?.();
                });
            }

            // Find optimal runs beyond the chart range.
            if (findBestBtn) {
                findBestBtn.addEventListener('click', async () => {
                    if (inFlight) return;

                    const statusEl = document.getElementById('run-optimized-status');
                    const modeToggle = state.ui?.modeToggleEl;
                    const mode = (modeToggle && modeToggle.checked) ? 'dashboard' : 'optimize';

                    // Make sure we have a price snapshot.
                    if (!state.pricesSnapshot) {
                        await state.computeAndRenderCurve?.();
                    }

                    const maxValue = Math.max(1, Math.floor(Number(state.ui?.searchMaxRunsEl?.value || 0) || 1));
                    const decisions = (mode === 'dashboard')
                        ? (state.decisions || getCurrentDecisionsFromSimulationOrDom())
                        : null;

                    try {
                        if (statusEl) {
                            statusEl.className = 'alert alert-info mb-3';
                            statusEl.textContent = __(`Searching optimal runs up to ${maxValue}…`);
                        }

                        const res = await findOptimalRuns({
                            searchMaxRuns: maxValue,
                            pricesSnapshot: state.pricesSnapshot,
                            mode,
                            decisions,
                            statusEl,
                        });

                        renderBestRunSummary(state.ui?.bestResultEl, res.best, __(`Best up to ${maxValue}`));
                        renderBestRunConfigDetails(res.best);
                        if (statusEl) {
                            statusEl.className = 'alert alert-success mb-3';
                            statusEl.textContent = __(`Optimal runs found (samples=${res.samples}).`);
                        }
                    } catch (e) {
                        console.error('[IndyHub] Run optimized best-runs search failed', e);
                        if (statusEl) {
                            statusEl.className = 'alert alert-warning mb-3';
                            statusEl.textContent = __('Failed to search optimal runs.');
                        }
                    }
                });
            }

            initialized = true;
        }

            await computeAndRenderCurve();
        });
    }

    /**
 * Collect current buy/craft decisions from the tree
 */
function getCurrentBuyCraftDecisions() {
    const buyDecisions = [];

    // Traverse the material tree and collect items marked for buying
    document.querySelectorAll('.mat-switch').forEach(function(switchEl) {
        const typeId = switchEl.getAttribute('data-type-id');
        if (!switchEl.checked) { // Unchecked means "buy" instead of "craft"
            buyDecisions.push(typeId);
        }
    });

    return buyDecisions;
}

/**
 * Update blueprint configurations based on buy/craft decisions
 * DISABLED - Now handled by template logic to prevent page reloads
 */
function updateBuyCraftDecisions() {
    // DISABLED - This function used to reload the page on every switch change
    // Now the template handles switch changes with immediate visual updates
    // and deferred URL/database updates when changing tabs
    craftBPDebugLog('updateBuyCraftDecisions: Disabled - handled by template logic');
}

/**
 * Restore buy/craft switch states from URL parameters
 */
function restoreBuyCraftStateFromURL() {
    const urlParams = new URLSearchParams(window.location.search);
    const buyList = urlParams.get('buy');

    if (buyList) {
        const buyDecisions = buyList.split(',').map(id => id.trim()).filter(id => id);
        craftBPDebugLog('Restoring buy decisions from URL:', buyDecisions);

        // Set all switches to default (checked = craft)
        document.querySelectorAll('.mat-switch').forEach(function(switchEl) {
            switchEl.checked = true; // Default to craft
            updateSwitchLabel(switchEl);
        });

        // Set switches for buy decisions to unchecked
        buyDecisions.forEach(function(typeId) {
            const switchEl = document.querySelector(`.mat-switch[data-type-id="${typeId}"]`);
            if (switchEl) {
                switchEl.checked = false; // Set to buy
                updateSwitchLabel(switchEl);
            }
        });

        // Trigger visual updates for tree hierarchy (children switches)
        // Use setTimeout to ensure all switches are set before updating visuals
        setTimeout(function() {
            if (typeof window.refreshTreeSwitchHierarchy === 'function') {
                window.refreshTreeSwitchHierarchy();
            }
            if (window.SimulationAPI && typeof window.SimulationAPI.refreshFromDom === 'function') {
                window.SimulationAPI.refreshFromDom();
            }
            refreshTabsAfterStateChange();
        }, 100);
    }
}

/**
 * Update the label next to a switch based on its state
 */
function updateSwitchLabel(switchEl) {
    const group = switchEl.closest('.mat-switch-group');
    if (!group) {
        return;
    }
    const label = group.querySelector('.mode-label');
    if (!label) {
        return;
    }

    label.className = 'mode-label badge px-2 py-1 fw-bold';

    const isLockedByParent = switchEl.dataset.lockedByParent === 'true' && switchEl.disabled;

    if (switchEl.dataset.fixedMode === 'useless' || switchEl.dataset.userState === 'useless') {
        label.textContent = __('Useless');
        label.classList.add('bg-secondary', 'text-white');
        label.removeAttribute('title');
        return;
    }

    if (isLockedByParent) {
        label.textContent = __('Parent Buy');
        label.classList.add('bg-secondary', 'text-white');
        label.setAttribute('title', __('Inherited mode: a parent is set to Buy'));
        return;
    }

    if (switchEl.checked) {
        label.textContent = __('Prod');
        label.classList.add('bg-success', 'text-white');
    } else {
        label.textContent = __('Buy');
        label.classList.add('bg-danger', 'text-white');
    }

    label.removeAttribute('title');
}

/**
 * Initialize collapse/expand handlers for sub-levels
 */
function initializeCollapseHandlers() {
    document.querySelectorAll('.toggle-subtree').forEach(function(btn) {
        btn.addEventListener('click', function() {
            var targetId = btn.getAttribute('data-target');
            var subtree = document.getElementById(targetId);
            var icon = btn.querySelector('i');
            if (subtree) {
                var expanded = btn.getAttribute('aria-expanded') === 'true';
                subtree.classList.toggle('show', !expanded);
                btn.setAttribute('aria-expanded', !expanded);
                if (!expanded) {
                    icon.classList.remove('fa-chevron-right');
                    icon.classList.add('fa-chevron-down');
                } else {
                    icon.classList.remove('fa-chevron-down');
                    icon.classList.add('fa-chevron-right');
                }
            }
        });
    });

    const treeTab = document.getElementById('tab-tree');
    if (treeTab && !treeTab.dataset.summaryIconsInitialized) {
        treeTab.dataset.summaryIconsInitialized = 'true';
        treeTab.addEventListener('toggle', function(event) {
            if (event.target && event.target.tagName === 'DETAILS') {
                updateDetailsCaret(event.target);
            }
        });
        refreshTreeSummaryIcons();
    }

    const expandBtn = document.getElementById('expand-tree');
    if (expandBtn) {
        expandBtn.addEventListener('click', function() {
            expandAllTreeNodes();
        });
    }

    const collapseBtn = document.getElementById('collapse-tree');
    if (collapseBtn) {
        collapseBtn.addEventListener('click', function() {
            collapseAllTreeNodes();
        });
    }

    const setProdBtn = document.getElementById('set-tree-prod');
    if (setProdBtn) {
        setProdBtn.addEventListener('click', function() {
            setTreeModeForAll('prod');
        });
    }

    const setBuyBtn = document.getElementById('set-tree-buy');
    if (setBuyBtn) {
        setBuyBtn.addEventListener('click', function() {
            setTreeModeForAll('buy');
        });
    }

    const optimizeBtn = document.getElementById('optimize-profit');
    if (optimizeBtn) {
        optimizeBtn.addEventListener('click', function() {
            optimizeProfitabilityConfig();
        });
    }
}

/**
 * Initialize financial calculations
 */
function initializeFinancialCalculations() {
    // On change recalc (use real-price and sale-price-unit)
    const recalcInputs = Array.from(document.querySelectorAll('.real-price, .sale-price-unit'));
    recalcInputs.forEach(inp => {
        attachPriceInputListener(inp);

        if (inp.dataset.userModified === 'true') {
            updatePriceInputManualState(inp, true);
        }
    });

    const recalcNowBtn = document.getElementById('recalcNowBtn');
    if (recalcNowBtn) {
        recalcNowBtn.addEventListener('click', () => {
            recalcNowBtn.classList.add('pulse');
            window.CraftBP.refreshFinancials();
            window.setTimeout(() => recalcNowBtn.classList.remove('pulse'), 600);
        });
    }

    // Batch fetch Fuzzwork prices for display (fuzzwork-price and sale-price-unit), only include valid positive type IDs
    const fetchInputs = Array.from(document.querySelectorAll('input.fuzzwork-price[data-type-id], input.sale-price-unit[data-type-id]'))
        .filter(inp => {
            const id = parseInt(inp.getAttribute('data-type-id'), 10);
            return id > 0;
        });
    let typeIds = fetchInputs.map(inp => inp.getAttribute('data-type-id')).filter(Boolean);

    // Also fetch prices for *all* typeIds in the production tree so:
    // - optimizer can always compare buy vs prod
    // - surplus valuation can price any produced surplus item
    const treeTypeIds = [];
    try {
        const tree = window.BLUEPRINT_DATA?.materials_tree;
        const seen = new Set();
        const walk = (nodes) => {
            (Array.isArray(nodes) ? nodes : []).forEach(node => {
                const tid = String(Number(node?.type_id || node?.typeId || 0) || '').trim();
                if (tid && tid !== '0' && !seen.has(tid)) {
                    seen.add(tid);
                    treeTypeIds.push(tid);
                }
                const kids = node && (node.sub_materials || node.subMaterials);
                if (Array.isArray(kids) && kids.length) {
                    walk(kids);
                }
            });
        };
        walk(tree);
    } catch (e) {
        // ignore
    }

    // Include the final product type_id
    if (CRAFT_BP.productTypeId && !typeIds.includes(CRAFT_BP.productTypeId)) {
        typeIds.push(CRAFT_BP.productTypeId);
    }
    typeIds = [...new Set([...typeIds, ...treeTypeIds])];

    function stashExtraFuzzworkPrices(prices) {
        if (!window.SimulationAPI || typeof window.SimulationAPI.setPrice !== 'function') {
            return;
        }
        treeTypeIds.forEach(tid => {
            const raw = prices[tid] ?? prices[String(parseInt(tid, 10))];
            const price = raw != null ? (parseFloat(raw) || 0) : 0;
            if (price > 0) {
                window.SimulationAPI.setPrice(tid, 'fuzzwork', price);
            }
        });
    }

    fetchAllPrices(typeIds).then(prices => {
        populatePrices(fetchInputs, prices);
        stashExtraFuzzworkPrices(prices);
        recalcFinancials();
    });

    // Ensure the Financial tab list ordering matches the dashboard ordering on first load.
    // We intentionally do NOT call refreshTabsAfterStateChange() here, because that would
    // re-render the dashboard Materials pane.
    try {
        if (typeof updateFinancialTabFromState === 'function') {
            updateFinancialTabFromState();
        }
    } catch (e) {
        // ignore
    }

    // Bind Load Fuzzwork Prices button
    const loadBtn = document.getElementById('loadFuzzworkBtn');
    if (loadBtn) {
        loadBtn.addEventListener('click', function() {
            fetchAllPrices(typeIds).then(prices => {
                populatePrices(fetchInputs, prices);
                stashExtraFuzzworkPrices(prices);
                recalcFinancials();
            });
        });
    }

    const resetBtn = document.getElementById('resetManualPricesBtn');
    if (resetBtn) {
        resetBtn.addEventListener('click', () => {
            const priceInputs = document.querySelectorAll('.real-price[data-type-id], .sale-price-unit[data-type-id]');
            priceInputs.forEach(input => {
                const tid = input.getAttribute('data-type-id');
                if (input.classList.contains('sale-price-unit')) {
                    const fuzzInp = document.querySelector(`.fuzzwork-price[data-type-id="${tid}"]`);
                    input.value = (fuzzInp ? (fuzzInp.value || '0') : '0');
                    updatePriceInputManualState(input, false);

                    if (window.SimulationAPI && typeof window.SimulationAPI.setPrice === 'function' && tid) {
                        window.SimulationAPI.setPrice(tid, 'sale', parseFloat(input.value) || 0);
                    }
                } else {
                    // Real price resets to 0; calculations fall back to fuzzwork.
                    input.value = '0';
                    updatePriceInputManualState(input, false);

                    if (window.SimulationAPI && typeof window.SimulationAPI.setPrice === 'function' && tid) {
                        window.SimulationAPI.setPrice(tid, 'real', 0);
                    }
                }
            });

            recalcFinancials();
            if (window.CraftBP && typeof window.CraftBP.pushStatus === 'function') {
                window.CraftBP.pushStatus(__('Manual overrides reset'), 'info');
            }
        });
    }

    // Initialize purchase list computation
    const computeButton = document.getElementById('compute-needed');
    if (computeButton) {
        computeButton.addEventListener('click', computeNeededPurchases);
    }

    // Initialize Configure tab change handlers (ME/TE/BP usage/build environment)
    initializeMETEHandlers();
    initializeBuildEnvironmentSystemSync();
    initializeIndustryFeeSystemIdPrefill();
}

/**
 * Initialize Configure tab change handlers
 */
function initializeMETEHandlers() {
    // Flag to track pending Configure changes
    window.craftBPFlags = window.craftBPFlags || {};
    window.craftBPFlags.hasPendingMETEChanges = false;

    // Restore Configure settings from localStorage on page load
    restoreMETEFromLocalStorage(getScopedCraftStorageKey('craft_bp_config'));
    updateBuildEnvironmentSummary();

    function saveMETEToLocalStorage() {
        const config = getCurrentMETEConfig();
        try {
            const storageKey = getScopedCraftStorageKey('craft_bp_config');
            localStorage.setItem(storageKey, JSON.stringify(config));
            craftBPDebugLog('Configure settings saved to localStorage');
        } catch (error) {
            console.error('Error saving to localStorage:', error);
        }
    }

    function markMETEChanges() {
        // Save to localStorage immediately
        saveMETEToLocalStorage();

        if (!window.craftBPFlags.hasPendingMETEChanges) {
            window.craftBPFlags.hasPendingMETEChanges = true;
            craftBPDebugLog('Configure changes detected - will apply on tab change');

            // Visual feedback: add a subtle indicator that changes are pending
            const configTab = document.querySelector('#configure-tab-btn');
            if (configTab && !configTab.querySelector('.pending-changes-indicator')) {
                const indicator = document.createElement('span');
                indicator.className = 'pending-changes-indicator badge bg-warning text-dark ms-2';
                indicator.textContent = '*';
                indicator.title = __('Changes will apply when switching tabs');
                configTab.appendChild(indicator);
            }
        }
        updateBuildEnvironmentSummary();
    }

    // Listen to Configure inputs (ME/TE + Use BP + build environment) and schedule apply on tab change.
    const configureInputs = document.querySelectorAll(
        '#configure-pane input[name^="me_"], #configure-pane input[name^="te_"], #configure-pane input.bp-use-input[data-blueprint-type-id], #configure-pane [data-build-env-input="1"], #configure-pane [data-industry-fee-input="1"]'
    );
    craftBPDebugLog(`Found ${configureInputs.length} Configure inputs to monitor for changes`);

    configureInputs.forEach(input => {
        input.addEventListener('input', markMETEChanges);
        input.addEventListener('change', markMETEChanges);
        craftBPDebugLog(`Added listeners to ${input.name} input`);
    });

    // Also listen to main runs input change
    const runsInput = document.getElementById('runsInput');
    if (runsInput) {
        runsInput.addEventListener('input', markMETEChanges);
        runsInput.addEventListener('change', markMETEChanges);
        craftBPDebugLog('Added listeners to runs input');
    }

    // Listen to tab changes to apply pending ME/TE changes
    const tabButtons = document.querySelectorAll('#craftMainTabs button[data-bs-toggle="tab"]');
    tabButtons.forEach(button => {
        button.addEventListener('shown.bs.tab', function(event) {
            const targetTab = event.target.getAttribute('data-tab-name');
            craftBPDebugLog(`Tab switched to: ${targetTab}`);

            // If we're leaving the Configure tab and have pending changes, apply them
            if (targetTab !== 'configure' && window.craftBPFlags?.hasPendingMETEChanges) {
                window.craftBPFlags.switchingToTab = targetTab;
                craftBPDebugLog('Applying pending Configure changes...');
                applyPendingMETEChanges();
            }
        });
    });
    craftBPDebugLog(`Added tab change listeners to ${tabButtons.length} tabs`);
}

/**
 * Restore ME/TE configuration from localStorage
 */
function parseUseBlueprintIdsFromUrl() {
    const ids = new Set();
    try {
        const params = new URLSearchParams(window.location.search || '');
        const packed = String(params.get('use_bp') || '').trim();
        if (packed) {
            packed.split(',').forEach((raw) => {
                const id = Number(String(raw || '').trim()) || 0;
                if (id > 0) {
                    ids.add(id);
                }
            });
        }
    } catch (e) {
        // ignore
    }
    return ids;
}

function parseBuildEnvironmentFromUrl() {
    const parsed = { hasAny: false };

    try {
        const params = new URLSearchParams(window.location.search || '');
        parsed.hasAny = (
            params.has('build_system')
            || params.has('build_system_id')
            || params.has('build_system_index')
            || params.has('build_structure')
            || params.has('build_structure_id')
            || params.has('build_rigs')
        );
        if (!parsed.hasAny) {
            return parsed;
        }

        if (params.has('build_system')) {
            parsed.system = String(params.get('build_system') || '').trim().slice(0, 64);
        }
        if (params.has('build_system_id')) {
            const raw = Number(params.get('build_system_id') || 0);
            if (Number.isFinite(raw) && raw > 0) {
                parsed.systemId = Math.floor(raw);
            }
        }
        if (params.has('build_system_index')) {
            const raw = Number(params.get('build_system_index') || 0);
            if (Number.isFinite(raw)) {
                parsed.systemIndex = Math.max(0, Math.min(raw, 100));
            }
        }
        if (params.has('build_structure')) {
            const structure = String(params.get('build_structure') || '').trim().toLowerCase();
            parsed.structureType = structure || 'none';
        }
        if (params.has('build_structure_id')) {
            const raw = Number(params.get('build_structure_id') || 0);
            if (Number.isFinite(raw) && raw > 0) {
                parsed.structureId = Math.floor(raw);
            }
        }
        if (params.has('build_rigs')) {
            parsed.rigKeys = [];
            const packed = String(params.get('build_rigs') || '').trim();
            if (packed) {
                const seen = new Set();
                packed.split(',').forEach((raw) => {
                    const key = String(raw || '').trim().toLowerCase();
                    if (!key || seen.has(key)) {
                        return;
                    }
                    seen.add(key);
                    parsed.rigKeys.push(key);
                });
            }
        }
    } catch (e) {
        // ignore
    }

    return parsed;
}

function applyBuildEnvironmentToDom(environment) {
    if (!environment || typeof environment !== 'object') {
        return;
    }

    const systemInput = document.getElementById('buildSystemInput');
    if (systemInput && typeof environment.system === 'string') {
        systemInput.value = environment.system;
    }

    const systemIdInput = document.getElementById('buildSystemIdInput');
    if (systemIdInput && environment.systemId !== undefined && environment.systemId !== null && environment.systemId !== '') {
        const numeric = Number(environment.systemId);
        if (Number.isFinite(numeric) && numeric > 0) {
            systemIdInput.value = String(Math.floor(numeric));
        }
    }

    const systemIndexInput = document.getElementById('buildSystemIndexInput');
    if (systemIndexInput && environment.systemIndex !== undefined && environment.systemIndex !== null && environment.systemIndex !== '') {
        const numeric = Number(environment.systemIndex);
        if (Number.isFinite(numeric)) {
            systemIndexInput.value = String(Math.max(0, Math.min(numeric, 100)));
        }
    }

    const structureSelect = document.getElementById('buildStructureSelect');
    const structureIdInput = document.getElementById('buildStructureIdInput');
    if (structureSelect) {
        const requestedStructureId = Number(environment.structureId) || 0;
        const requestedStructureType = String(environment.structureType || '').trim().toLowerCase();
        let option = null;
        if (requestedStructureId > 0) {
            option = Array.from(structureSelect.options || []).find((opt) => {
                return (Number(opt.getAttribute('data-structure-id')) || 0) === requestedStructureId;
            });
        }
        if (!option && requestedStructureType) {
            option = Array.from(structureSelect.options || []).find((opt) => {
                const typeKey = String(opt.getAttribute('data-structure-type') || opt.value || '').trim().toLowerCase();
                return typeKey === requestedStructureType;
            });
        }
        structureSelect.value = option ? option.value : 'none';
        if (structureIdInput) {
            structureIdInput.value = requestedStructureId > 0 ? String(requestedStructureId) : '';
        }
    }

    if (Array.isArray(environment.rigKeys)) {
        const activeKeys = new Set(
            environment.rigKeys
                .map((key) => String(key || '').trim().toLowerCase())
                .filter(Boolean)
        );
        document.querySelectorAll('#configure-pane .build-rig-input[data-rig-key]').forEach((input) => {
            const key = String(input.getAttribute('data-rig-key') || '').trim().toLowerCase();
            input.checked = activeKeys.has(key);
        });
    }
}

function getBuildEnvironmentFromDom() {
    const structureSelect = document.getElementById('buildStructureSelect');
    const selectedStructureOption = structureSelect
        ? structureSelect.options[structureSelect.selectedIndex]
        : null;
    const structureType = selectedStructureOption
        ? String(selectedStructureOption.getAttribute('data-structure-type') || selectedStructureOption.value || 'none').trim().toLowerCase()
        : (
            structureSelect
                ? String(structureSelect.value || 'none').trim().toLowerCase()
                : 'none'
        );
    const structureIdInput = document.getElementById('buildStructureIdInput');
    const structureId = selectedStructureOption
        ? (Number(selectedStructureOption.getAttribute('data-structure-id')) || 0)
        : 0;
    const fallbackStructureId = Number(structureIdInput ? structureIdInput.value : 0) || 0;
    const structureBonus = selectedStructureOption
        ? (Number(selectedStructureOption.getAttribute('data-material-bonus') || 0) || 0)
        : 0;

    const rigKeys = [];
    let rigBonus = 0;
    document.querySelectorAll('#configure-pane .build-rig-input[data-rig-key]').forEach((input) => {
        if (!input.checked) {
            return;
        }
        const key = String(input.getAttribute('data-rig-key') || '').trim().toLowerCase();
        if (key) {
            rigKeys.push(key);
        }
        const bonus = Number(input.getAttribute('data-material-bonus') || 0) || 0;
        if (bonus > rigBonus) {
            rigBonus = bonus;
        }
    });

    const systemInput = document.getElementById('buildSystemInput');
    const systemIdInput = document.getElementById('buildSystemIdInput');
    const systemIndexInput = document.getElementById('buildSystemIndexInput');
    const rawSystemId = Number(systemIdInput ? systemIdInput.value : 0);
    const rawSystemIndex = Number(systemIndexInput ? systemIndexInput.value : 0);
    const systemId = Number.isFinite(rawSystemId) && rawSystemId > 0
        ? Math.floor(rawSystemId)
        : 0;
    const systemIndex = Number.isFinite(rawSystemIndex)
        ? Math.max(0, Math.min(rawSystemIndex, 100))
        : 0;
    const effectiveBonus = 1 - ((1 - structureBonus) * (1 - rigBonus));

    return {
        system: String(systemInput ? systemInput.value : '').trim().slice(0, 64),
        systemId,
        systemIndex,
        structureType: structureType || 'none',
        structureId: structureId > 0 ? structureId : (fallbackStructureId > 0 ? fallbackStructureId : 0),
        structureMaterialBonus: Math.max(0, structureBonus),
        rigKeys: Array.from(new Set(rigKeys)),
        rigMaterialBonus: Math.max(0, rigBonus),
        effectiveMaterialBonus: Math.max(0, effectiveBonus),
    };
}

function updateBuildEnvironmentSummary() {
    const environment = getBuildEnvironmentFromDom();
    const structureEl = document.getElementById('buildStructureBonusValue');
    const rigEl = document.getElementById('buildRigBonusValue');
    const effectiveEl = document.getElementById('buildEffectiveMaterialBonusValue');

    if (structureEl) {
        structureEl.textContent = `${(environment.structureMaterialBonus * 100).toFixed(2)}%`;
    }
    if (rigEl) {
        rigEl.textContent = `${(environment.rigMaterialBonus * 100).toFixed(2)}%`;
    }
    if (effectiveEl) {
        effectiveEl.textContent = `${(environment.effectiveMaterialBonus * 100).toFixed(2)}%`;
    }
}

function getCraftBuildEnvironmentApiUrl() {
    return String(window.BLUEPRINT_DATA?.urls?.craft_build_environment || '').trim();
}

function parseCommaList(value) {
    return String(value || '')
        .split(',')
        .map((token) => String(token || '').trim())
        .filter(Boolean);
}

function parseCommaIntList(value) {
    return parseCommaList(value)
        .map((token) => Number(token) || 0)
        .filter((valueInt) => valueInt > 0);
}

function applySelectedStructureDerivedSettings() {
    const structureSelect = document.getElementById('buildStructureSelect');
    const structureIdInput = document.getElementById('buildStructureIdInput');
    const selectedOption = structureSelect
        ? structureSelect.options[structureSelect.selectedIndex]
        : null;

    const rigKeys = selectedOption ? parseCommaList(selectedOption.getAttribute('data-rig-keys')) : [];
    const rigKeySet = new Set(rigKeys.map((key) => key.toLowerCase()));
    const rigTypeIds = selectedOption ? parseCommaIntList(selectedOption.getAttribute('data-rig-type-ids')) : [];
    const structureTypeId = selectedOption
        ? (Number(selectedOption.getAttribute('data-structure-type-id')) || 0)
        : 0;
    const selectedStructureId = selectedOption
        ? (Number(selectedOption.getAttribute('data-structure-id')) || 0)
        : 0;
    const facilityTaxAttr = selectedOption
        ? String(selectedOption.getAttribute('data-facility-tax') || '').trim()
        : '';
    const facilityTaxParsed = Number(facilityTaxAttr);
    const facilityTax = (
        facilityTaxAttr !== ''
        && Number.isFinite(facilityTaxParsed)
        && facilityTaxParsed >= 0
    )
        ? facilityTaxParsed
        : null;

    if (structureIdInput) {
        structureIdInput.value = selectedStructureId > 0 ? String(selectedStructureId) : '';
    }

    const rigInputs = document.querySelectorAll('#configure-pane .build-rig-input[data-rig-key]');
    rigInputs.forEach((input) => {
        const key = String(input.getAttribute('data-rig-key') || '').trim().toLowerCase();
        input.disabled = true;
        input.checked = rigKeySet.size > 0 && rigKeySet.has(key);
    });

    const structureTypeIdInput = document.getElementById('industryFeeStructureTypeIdInput');
    if (structureTypeIdInput) {
        structureTypeIdInput.value = structureTypeId > 0 ? String(structureTypeId) : '';
        structureTypeIdInput.dataset.autoFilled = '1';
    }

    const rigTypeIdsInput = document.getElementById('industryFeeRigIdsInput');
    if (rigTypeIdsInput) {
        rigTypeIdsInput.value = rigTypeIds.length > 0 ? rigTypeIds.join(',') : '';
        rigTypeIdsInput.dataset.autoFilled = '1';
    }

    const facilityTaxInput = document.getElementById('industryFeeFacilityTaxInput');
    if (facilityTaxInput) {
        const canAutoFill = facilityTaxInput.dataset.autoFilled !== '0';
        if (facilityTax !== null) {
            if (canAutoFill) {
                facilityTaxInput.value = String(facilityTax);
                facilityTaxInput.dataset.autoFilled = '1';
            }
        } else if (facilityTaxInput.dataset.autoFilled === '1') {
            facilityTaxInput.value = '';
            facilityTaxInput.dataset.autoFilled = '0';
        }
    }

    updateBuildEnvironmentSummary();
}

function renderSystemStructureOptions(structures, environment) {
    const structureSelect = document.getElementById('buildStructureSelect');
    if (!structureSelect) {
        return;
    }

    const preferredStructureId = Number(environment?.structureId) || 0;
    const preferredStructureType = String(environment?.structureType || '').trim().toLowerCase();

    CRAFT_BUILD_SYSTEM_STATE.structuresById = new Map();

    const options = [];
    options.push({
        value: 'none',
        label: __('No structure selected'),
        structureId: 0,
        structureType: 'none',
        structureTypeId: 0,
        materialBonus: 0,
        rigKeys: [],
        rigTypeIds: [],
        facilityTax: null,
    });

    (Array.isArray(structures) ? structures : []).forEach((row) => {
        const structureId = Number(row?.structure_id) || 0;
        if (structureId <= 0) {
            return;
        }
        const structureType = String(row?.structure_type_key || '').trim().toLowerCase();
        const labelName = String(row?.structure_name || '').trim() || `Structure ${structureId}`;
        const typeName = String(row?.structure_type_name || '').trim();
        const materialBonus = Number(row?.material_bonus) || 0;
        const rigKeys = Array.from(new Set((Array.isArray(row?.rig_keys) ? row.rig_keys : []).map((key) => String(key || '').trim().toLowerCase()).filter(Boolean)));
        const rigTypeIds = Array.from(new Set((Array.isArray(row?.rig_type_ids) ? row.rig_type_ids : []).map((id) => Number(id) || 0).filter((id) => id > 0)));
        const structureTypeId = Number(row?.structure_type_id) || 0;
        const facilityTaxRaw = row?.facility_tax;
        const facilityTaxText = facilityTaxRaw === null || facilityTaxRaw === undefined
            ? ''
            : String(facilityTaxRaw).trim();
        const facilityTaxParsed = Number(facilityTaxText);
        const facilityTax = (
            facilityTaxText !== ''
            && Number.isFinite(facilityTaxParsed)
            && facilityTaxParsed >= 0
        )
            ? facilityTaxParsed
            : null;

        CRAFT_BUILD_SYSTEM_STATE.structuresById.set(structureId, row);
        options.push({
            value: `structure:${structureId}`,
            label: typeName ? `${labelName} (${typeName})` : labelName,
            structureId,
            structureType: structureType || 'none',
            structureTypeId,
            materialBonus: Math.max(0, materialBonus),
            rigKeys,
            rigTypeIds,
            facilityTax,
        });
    });

    structureSelect.innerHTML = '';
    options.forEach((optionData) => {
        const option = document.createElement('option');
        option.value = optionData.value;
        option.textContent = optionData.label;
        option.setAttribute('data-structure-id', optionData.structureId > 0 ? String(optionData.structureId) : '');
        option.setAttribute('data-structure-type', optionData.structureType || 'none');
        option.setAttribute('data-structure-type-id', optionData.structureTypeId > 0 ? String(optionData.structureTypeId) : '');
        option.setAttribute('data-material-bonus', String(optionData.materialBonus || 0));
        option.setAttribute('data-rig-keys', optionData.rigKeys.join(','));
        option.setAttribute('data-rig-type-ids', optionData.rigTypeIds.join(','));
        option.setAttribute(
            'data-facility-tax',
            optionData.facilityTax !== null && optionData.facilityTax !== undefined
                ? String(optionData.facilityTax)
                : ''
        );
        structureSelect.appendChild(option);
    });

    let selectedOptionValue = 'none';
    if (preferredStructureId > 0) {
        const byId = options.find((opt) => opt.structureId === preferredStructureId);
        if (byId) {
            selectedOptionValue = byId.value;
        }
    }
    if (selectedOptionValue === 'none' && preferredStructureType) {
        const byType = options.find((opt) => opt.structureType === preferredStructureType);
        if (byType) {
            selectedOptionValue = byType.value;
        }
    }
    structureSelect.value = selectedOptionValue;
    applySelectedStructureDerivedSettings();
}

function applyResolvedBuildSystemContext(context, environment) {
    const system = context && typeof context === 'object' ? context.system : null;
    if (!system || typeof system !== 'object') {
        return;
    }

    const systemName = String(system.system_name || '').trim();
    const systemId = Number(system.system_id) || 0;
    const manufacturingIndexPercent = Number(system.manufacturing_cost_percent);
    const manufacturingIndexFraction = Number(system.manufacturing_cost_index);
    const inventionIndexFraction = Number(system.invention_cost_index);
    const copyingIndexFraction = Number(system.copying_cost_index);
    const reactionIndexFraction = Number(system.reaction_cost_index);
    const securityClass = String(system.security_class || '').trim().toUpperCase();

    const buildSystemInput = document.getElementById('buildSystemInput');
    if (buildSystemInput && systemName) {
        buildSystemInput.value = systemName;
    }

    const buildSystemIdInput = document.getElementById('buildSystemIdInput');
    if (buildSystemIdInput) {
        buildSystemIdInput.value = systemId > 0 ? String(systemId) : '';
    }

    const buildSystemIndexInput = document.getElementById('buildSystemIndexInput');
    if (buildSystemIndexInput && Number.isFinite(manufacturingIndexPercent)) {
        buildSystemIndexInput.value = String(Math.max(0, manufacturingIndexPercent));
    }

    const industryFeeSystemIdInput = document.getElementById('industryFeeSystemIdInput');
    if (industryFeeSystemIdInput && systemId > 0) {
        industryFeeSystemIdInput.value = String(systemId);
        industryFeeSystemIdInput.dataset.autoFilled = '1';
    }

    const industryFeeSecurityInput = document.getElementById('industryFeeSecurityInput');
    const securityCanAutoFill = industryFeeSecurityInput?.dataset.autoFilled !== '0';
    if (industryFeeSecurityInput && securityClass && securityCanAutoFill) {
        industryFeeSecurityInput.value = securityClass;
        industryFeeSecurityInput.dataset.autoFilled = '1';
    }

    const industryFeeManufacturingInput = document.getElementById('industryFeeManufacturingCostInput');
    const manufacturingCanAutoFill = industryFeeManufacturingInput?.dataset.autoFilled !== '0';
    if (industryFeeManufacturingInput && Number.isFinite(manufacturingIndexFraction) && manufacturingCanAutoFill) {
        industryFeeManufacturingInput.value = String(Math.max(0, manufacturingIndexFraction));
        industryFeeManufacturingInput.dataset.autoFilled = '1';
    }

    const industryFeeInventionInput = document.getElementById('industryFeeInventionCostInput');
    const inventionCanAutoFill = industryFeeInventionInput?.dataset.autoFilled !== '0';
    if (industryFeeInventionInput && Number.isFinite(inventionIndexFraction) && inventionCanAutoFill) {
        industryFeeInventionInput.value = String(Math.max(0, inventionIndexFraction));
        industryFeeInventionInput.dataset.autoFilled = '1';
    }

    const industryFeeCopyingInput = document.getElementById('industryFeeCopyingCostInput');
    const copyingCanAutoFill = industryFeeCopyingInput?.dataset.autoFilled !== '0';
    if (industryFeeCopyingInput && Number.isFinite(copyingIndexFraction) && copyingCanAutoFill) {
        industryFeeCopyingInput.value = String(Math.max(0, copyingIndexFraction));
        industryFeeCopyingInput.dataset.autoFilled = '1';
    }

    const industryFeeReactionInput = document.getElementById('industryFeeReactionCostInput');
    const reactionCanAutoFill = industryFeeReactionInput?.dataset.autoFilled !== '0';
    if (industryFeeReactionInput && Number.isFinite(reactionIndexFraction) && reactionCanAutoFill) {
        industryFeeReactionInput.value = String(Math.max(0, reactionIndexFraction));
        industryFeeReactionInput.dataset.autoFilled = '1';
    }

    renderSystemStructureOptions(context.structures || [], environment);
}

function clearAutoFilledIndustryFeeSystemContext() {
    const industryFeeSystemIdInput = document.getElementById('industryFeeSystemIdInput');
    if (industryFeeSystemIdInput && industryFeeSystemIdInput.dataset.autoFilled === '1') {
        industryFeeSystemIdInput.value = '';
    }

    const industryFeeSecurityInput = document.getElementById('industryFeeSecurityInput');
    if (industryFeeSecurityInput && industryFeeSecurityInput.dataset.autoFilled === '1') {
        industryFeeSecurityInput.value = '';
    }

    const industryFeeManufacturingInput = document.getElementById('industryFeeManufacturingCostInput');
    if (industryFeeManufacturingInput && industryFeeManufacturingInput.dataset.autoFilled === '1') {
        industryFeeManufacturingInput.value = '';
    }

    const industryFeeInventionInput = document.getElementById('industryFeeInventionCostInput');
    if (industryFeeInventionInput && industryFeeInventionInput.dataset.autoFilled === '1') {
        industryFeeInventionInput.value = '';
    }

    const industryFeeCopyingInput = document.getElementById('industryFeeCopyingCostInput');
    if (industryFeeCopyingInput && industryFeeCopyingInput.dataset.autoFilled === '1') {
        industryFeeCopyingInput.value = '';
    }

    const industryFeeReactionInput = document.getElementById('industryFeeReactionCostInput');
    if (industryFeeReactionInput && industryFeeReactionInput.dataset.autoFilled === '1') {
        industryFeeReactionInput.value = '';
    }

    const industryFeeFacilityTaxInput = document.getElementById('industryFeeFacilityTaxInput');
    if (industryFeeFacilityTaxInput && industryFeeFacilityTaxInput.dataset.autoFilled === '1') {
        industryFeeFacilityTaxInput.value = '';
    }
}

async function syncBuildEnvironmentFromSystem(options = {}) {
    const apiUrl = getCraftBuildEnvironmentApiUrl();
    if (!apiUrl) {
        return;
    }

    const environment = getBuildEnvironmentFromDom();
    const systemText = String(environment.system || '').trim();
    const systemId = Number(environment.systemId) || 0;
    if (!systemText && systemId <= 0) {
        return;
    }

    const signature = `${systemText}|${systemId}`;
    CRAFT_BUILD_SYSTEM_STATE.lastSignature = signature;
    CRAFT_BUILD_SYSTEM_STATE.loading = true;

    try {
        const url = new URL(apiUrl, window.location.origin);
        if (systemText) {
            url.searchParams.set('system', systemText);
        }
        if (systemId > 0) {
            url.searchParams.set('system_id', String(systemId));
        }
        if (Number(environment.structureId) > 0) {
            url.searchParams.set('structure_id', String(environment.structureId));
        }

        const response = await fetch(url.toString(), {
            headers: { 'X-Requested-With': 'XMLHttpRequest' },
        });
        if (!response.ok) {
            throw new Error(`Request failed with status ${response.status}`);
        }
        const payload = await response.json();
        if (signature !== CRAFT_BUILD_SYSTEM_STATE.lastSignature) {
            return;
        }
        if (!payload || payload.resolved !== true) {
            clearAutoFilledIndustryFeeSystemContext();
            return;
        }

        applyResolvedBuildSystemContext(payload, environment);
        if (options.markChanged !== false && typeof updateBuildEnvironmentSummary === 'function') {
            updateBuildEnvironmentSummary();
        }
    } catch (error) {
        console.error('[CraftBP] Failed resolving build environment', error);
    } finally {
        if (signature === CRAFT_BUILD_SYSTEM_STATE.lastSignature) {
            CRAFT_BUILD_SYSTEM_STATE.loading = false;
        }
    }
}

function initializeBuildEnvironmentSystemSync() {
    const buildSystemInput = document.getElementById('buildSystemInput');
    const buildSystemIdInput = document.getElementById('buildSystemIdInput');
    const buildStructureSelect = document.getElementById('buildStructureSelect');
    const buildStructureIdInput = document.getElementById('buildStructureIdInput');
    if (!buildSystemInput || !buildStructureSelect) {
        return;
    }

    const scheduleSync = () => {
        if (buildSystemIdInput) {
            buildSystemIdInput.value = '';
        }
        const buildSystemIndexInput = document.getElementById('buildSystemIndexInput');
        if (buildSystemIndexInput) {
            buildSystemIndexInput.value = '';
        }
        if (buildStructureIdInput) {
            buildStructureIdInput.value = '';
        }
        clearAutoFilledIndustryFeeSystemContext();
        if (buildStructureSelect) {
            buildStructureSelect.value = 'none';
            applySelectedStructureDerivedSettings();
        }
        if (CRAFT_BUILD_SYSTEM_STATE.debounceTimer) {
            window.clearTimeout(CRAFT_BUILD_SYSTEM_STATE.debounceTimer);
        }
        CRAFT_BUILD_SYSTEM_STATE.debounceTimer = window.setTimeout(() => {
            syncBuildEnvironmentFromSystem();
        }, 350);
    };

    buildSystemInput.addEventListener('input', scheduleSync);
    buildSystemInput.addEventListener('change', () => {
        if (CRAFT_BUILD_SYSTEM_STATE.debounceTimer) {
            window.clearTimeout(CRAFT_BUILD_SYSTEM_STATE.debounceTimer);
        }
        syncBuildEnvironmentFromSystem();
    });
    if (buildSystemIdInput) {
        buildSystemIdInput.addEventListener('change', scheduleSync);
    }

    buildStructureSelect.addEventListener('change', () => {
        applySelectedStructureDerivedSettings();
    });

    syncBuildEnvironmentFromSystem({ markChanged: false });
}

function parseIndustryFeeConfigFromUrl() {
    const parsed = { hasAny: false };
    try {
        const params = new URLSearchParams(window.location.search || '');
        const knownKeys = [
            'industry_fee_enabled',
            'industry_fee_structure_type_id',
            'industry_fee_security',
            'industry_fee_system_id',
            'industry_fee_manufacturing_cost',
            'industry_fee_invention_cost',
            'industry_fee_copying_cost',
            'industry_fee_reaction_cost',
            'industry_fee_facility_tax',
            'industry_fee_system_cost_bonus',
            'industry_fee_rig_ids',
            'industry_fee_alpha',
            'industry_fee_extra_params',
        ];
        parsed.hasAny = knownKeys.some((key) => params.has(key));
        if (!parsed.hasAny) {
            return parsed;
        }

        const parseMaybeNumber = (value) => {
            const numeric = Number(value);
            return Number.isFinite(numeric) ? numeric : null;
        };

        parsed.enabled = ['1', 'true', 'yes', 'on'].includes(String(params.get('industry_fee_enabled') || '').trim().toLowerCase());
        parsed.structure_type_id = parseMaybeNumber(params.get('industry_fee_structure_type_id'));
        parsed.security = String(params.get('industry_fee_security') || 'HIGH_SEC').trim().toUpperCase();
        parsed.system_id = parseMaybeNumber(params.get('industry_fee_system_id'));
        parsed.manufacturing_cost = parseMaybeNumber(params.get('industry_fee_manufacturing_cost'));
        parsed.invention_cost = parseMaybeNumber(params.get('industry_fee_invention_cost'));
        parsed.copying_cost = parseMaybeNumber(params.get('industry_fee_copying_cost'));
        parsed.reaction_cost = parseMaybeNumber(params.get('industry_fee_reaction_cost'));
        parsed.facility_tax = parseMaybeNumber(params.get('industry_fee_facility_tax'));
        parsed.system_cost_bonus = parseMaybeNumber(params.get('industry_fee_system_cost_bonus'));
        parsed.rig_ids = String(params.get('industry_fee_rig_ids') || '')
            .split(',')
            .map((token) => Number(String(token || '').trim()) || 0)
            .filter((id) => id > 0);
        parsed.alpha = ['1', 'true', 'yes', 'on'].includes(String(params.get('industry_fee_alpha') || '').trim().toLowerCase());
        parsed.extra_params = String(params.get('industry_fee_extra_params') || '').trim();
    } catch (e) {
        // ignore
    }
    return parsed;
}

function getIndustryFeeConfigFromDom() {
    const getNumber = (id) => {
        const input = document.getElementById(id);
        if (!input) {
            return null;
        }
        const raw = String(input.value || '').trim();
        if (!raw) {
            return null;
        }
        const value = Number(raw);
        return Number.isFinite(value) ? value : null;
    };

    const rigIds = String(document.getElementById('industryFeeRigIdsInput')?.value || '')
        .split(',')
        .map((token) => Number(String(token || '').trim()) || 0)
        .filter((id) => id > 0);

    return {
        enabled: Boolean(document.getElementById('industryFeeEnabledInput')?.checked),
        structure_type_id: getNumber('industryFeeStructureTypeIdInput'),
        security: String(document.getElementById('industryFeeSecurityInput')?.value || 'HIGH_SEC').trim().toUpperCase(),
        system_id: (() => {
            const explicit = getNumber('industryFeeSystemIdInput');
            if (explicit) {
                return explicit;
            }
            return resolveBuildEnvironmentSystemId();
        })(),
        manufacturing_cost: getNumber('industryFeeManufacturingCostInput'),
        invention_cost: getNumber('industryFeeInventionCostInput'),
        copying_cost: getNumber('industryFeeCopyingCostInput'),
        reaction_cost: getNumber('industryFeeReactionCostInput'),
        facility_tax: getNumber('industryFeeFacilityTaxInput'),
        system_cost_bonus: getNumber('industryFeeSystemCostBonusInput'),
        rig_ids: Array.from(new Set(rigIds)),
        alpha: Boolean(document.getElementById('industryFeeAlphaInput')?.checked),
        extra_params: String(document.getElementById('industryFeeExtraParamsInput')?.value || '').trim(),
    };
}

function resolveBuildEnvironmentSystemId() {
    const buildSystemIdInput = document.getElementById('buildSystemIdInput');
    const explicitSystemId = Number(String(buildSystemIdInput?.value || '').trim());
    if (Number.isFinite(explicitSystemId) && explicitSystemId > 0) {
        return Math.floor(explicitSystemId);
    }
    return null;
}

function syncIndustryFeeSystemIdPrefill() {
    const systemIdInput = document.getElementById('industryFeeSystemIdInput');
    if (!systemIdInput) {
        return;
    }
    const hasExplicit = String(systemIdInput.value || '').trim() !== '';
    if (hasExplicit && systemIdInput.dataset.autoFilled !== '1') {
        return;
    }
    const resolvedId = resolveBuildEnvironmentSystemId();
    if (resolvedId && resolvedId > 0) {
        systemIdInput.value = String(resolvedId);
        systemIdInput.dataset.autoFilled = '1';
    } else if (systemIdInput.dataset.autoFilled === '1') {
        systemIdInput.value = '';
        systemIdInput.dataset.autoFilled = '0';
    }
}

function initializeIndustryFeeSystemIdPrefill() {
    const buildSystemInput = document.getElementById('buildSystemInput');
    const buildSystemIdInput = document.getElementById('buildSystemIdInput');
    const systemIdInput = document.getElementById('industryFeeSystemIdInput');
    if (!buildSystemInput || !systemIdInput) {
        return;
    }

    if (!String(systemIdInput.value || '').trim()) {
        systemIdInput.dataset.autoFilled = '1';
        syncIndustryFeeSystemIdPrefill();
    }

    buildSystemInput.addEventListener('input', syncIndustryFeeSystemIdPrefill);
    buildSystemInput.addEventListener('change', syncIndustryFeeSystemIdPrefill);
    if (buildSystemIdInput) {
        buildSystemIdInput.addEventListener('input', syncIndustryFeeSystemIdPrefill);
        buildSystemIdInput.addEventListener('change', syncIndustryFeeSystemIdPrefill);
    }
    systemIdInput.addEventListener('input', () => {
        systemIdInput.dataset.autoFilled = '0';
    });

    const securityInput = document.getElementById('industryFeeSecurityInput');
    if (securityInput) {
        securityInput.addEventListener('change', () => {
            securityInput.dataset.autoFilled = '0';
        });
        securityInput.addEventListener('input', () => {
            securityInput.dataset.autoFilled = '0';
        });
    }

    const manufacturingCostInput = document.getElementById('industryFeeManufacturingCostInput');
    if (manufacturingCostInput) {
        manufacturingCostInput.addEventListener('change', () => {
            manufacturingCostInput.dataset.autoFilled = '0';
        });
        manufacturingCostInput.addEventListener('input', () => {
            manufacturingCostInput.dataset.autoFilled = '0';
        });
    }

    const inventionCostInput = document.getElementById('industryFeeInventionCostInput');
    if (inventionCostInput) {
        inventionCostInput.addEventListener('change', () => {
            inventionCostInput.dataset.autoFilled = '0';
        });
        inventionCostInput.addEventListener('input', () => {
            inventionCostInput.dataset.autoFilled = '0';
        });
    }

    const copyingCostInput = document.getElementById('industryFeeCopyingCostInput');
    if (copyingCostInput) {
        copyingCostInput.addEventListener('change', () => {
            copyingCostInput.dataset.autoFilled = '0';
        });
        copyingCostInput.addEventListener('input', () => {
            copyingCostInput.dataset.autoFilled = '0';
        });
    }

    const reactionCostInput = document.getElementById('industryFeeReactionCostInput');
    if (reactionCostInput) {
        reactionCostInput.addEventListener('change', () => {
            reactionCostInput.dataset.autoFilled = '0';
        });
        reactionCostInput.addEventListener('input', () => {
            reactionCostInput.dataset.autoFilled = '0';
        });
    }

    const facilityTaxInput = document.getElementById('industryFeeFacilityTaxInput');
    if (facilityTaxInput) {
        facilityTaxInput.addEventListener('change', () => {
            facilityTaxInput.dataset.autoFilled = '0';
        });
        facilityTaxInput.addEventListener('input', () => {
            facilityTaxInput.dataset.autoFilled = '0';
        });
    }
}

function applyIndustryFeeConfigToDom(config) {
    if (!config || typeof config !== 'object') {
        return;
    }
    const setValue = (id, value) => {
        const input = document.getElementById(id);
        if (!input) {
            return;
        }
        input.value = value === null || value === undefined ? '' : String(value);
    };

    const enabledInput = document.getElementById('industryFeeEnabledInput');
    if (enabledInput && config.enabled !== undefined) {
        enabledInput.checked = Boolean(config.enabled);
    }
    setValue('industryFeeStructureTypeIdInput', config.structure_type_id);
    setValue('industryFeeSystemIdInput', config.system_id);
    setValue('industryFeeManufacturingCostInput', config.manufacturing_cost);
    setValue('industryFeeInventionCostInput', config.invention_cost);
    setValue('industryFeeCopyingCostInput', config.copying_cost);
    setValue('industryFeeReactionCostInput', config.reaction_cost);
    setValue('industryFeeFacilityTaxInput', config.facility_tax);
    setValue('industryFeeSystemCostBonusInput', config.system_cost_bonus);
    setValue('industryFeeRigIdsInput', Array.isArray(config.rig_ids) ? config.rig_ids.join(',') : '');
    setValue('industryFeeExtraParamsInput', config.extra_params || '');

    const securityInput = document.getElementById('industryFeeSecurityInput');
    if (securityInput && config.security) {
        securityInput.value = String(config.security).toUpperCase();
    }
    const alphaInput = document.getElementById('industryFeeAlphaInput');
    if (alphaInput && config.alpha !== undefined) {
        alphaInput.checked = Boolean(config.alpha);
    }
}

function restoreMETEFromLocalStorage(storageKey) {
    const urlUseIds = parseUseBlueprintIdsFromUrl();
    const urlBuildEnvironment = parseBuildEnvironmentFromUrl();
    const urlIndustryFee = parseIndustryFeeConfigFromUrl();

    try {
        const savedConfig = localStorage.getItem(storageKey);
        if (!savedConfig) {
            craftBPDebugLog('No saved Configure settings in localStorage');
            // Even without localStorage, URL can still carry BP selection.
            if (urlUseIds.size > 0) {
                document.querySelectorAll('#configure-pane input.bp-use-input[data-blueprint-type-id]').forEach((input) => {
                    const typeId = Number(input.getAttribute('data-blueprint-type-id')) || 0;
                    if (!typeId || input.disabled) {
                        return;
                    }
                    input.checked = urlUseIds.has(typeId);
                });
            }
            if (urlBuildEnvironment.hasAny) {
                applyBuildEnvironmentToDom(urlBuildEnvironment);
            }
            if (urlIndustryFee.hasAny) {
                applyIndustryFeeConfigToDom(urlIndustryFee);
            }
            updateBuildEnvironmentSummary();
            return;
        }

        const config = JSON.parse(savedConfig);
        craftBPDebugLog('Restoring Configure settings from localStorage:', config);

        // Apply saved values to inputs
        for (const [typeId, bpConfig] of Object.entries(config.blueprintConfigs || {})) {
            if (bpConfig.me !== undefined) {
                const meInput = document.querySelector(`input[name="me_${typeId}"]`);
                if (meInput && !meInput.value) {  // Only set if input is empty
                    meInput.value = bpConfig.me;
                }
            }
            if (bpConfig.te !== undefined) {
                const teInput = document.querySelector(`input[name="te_${typeId}"]`);
                if (teInput && !teInput.value) {  // Only set if input is empty
                    teInput.value = bpConfig.te;
                }
            }
            if (bpConfig.use !== undefined) {
                const useInput = document.querySelector(`#configure-pane input.bp-use-input[data-blueprint-type-id="${typeId}"]`);
                if (useInput && !useInput.disabled) {
                    useInput.checked = Boolean(bpConfig.use);
                }
            }
        }

        if (config.buildEnvironment && typeof config.buildEnvironment === 'object') {
            applyBuildEnvironmentToDom(config.buildEnvironment);
        }
        if (config.industryFee && typeof config.industryFee === 'object') {
            applyIndustryFeeConfigToDom(config.industryFee);
        }

        // URL override for easy sharing/bookmarking when present.
        if (urlUseIds.size > 0) {
            document.querySelectorAll('#configure-pane input.bp-use-input[data-blueprint-type-id]').forEach((input) => {
                const typeId = Number(input.getAttribute('data-blueprint-type-id')) || 0;
                if (!typeId || input.disabled) {
                    return;
                }
                input.checked = urlUseIds.has(typeId);
            });
        }
        if (urlBuildEnvironment.hasAny) {
            applyBuildEnvironmentToDom(urlBuildEnvironment);
        }
        if (urlIndustryFee.hasAny) {
            applyIndustryFeeConfigToDom(urlIndustryFee);
        }

        updateBuildEnvironmentSummary();
        craftBPDebugLog('Configure settings restored from localStorage');
    } catch (error) {
        console.error('Error restoring from localStorage:', error);
    }
}

/**
 * Apply pending ME/TE changes by recalculating via AJAX without page reload
 * Called when user switches away from Config tab
 */
function applyPendingMETEChanges() {
    if (!window.craftBPFlags?.hasPendingMETEChanges) {
        return false; // No changes to apply
    }

    craftBPDebugLog('Applying pending Configure changes by reloading page...');

    try {
        // Get current configuration values
        const config = getCurrentMETEConfig();
        const runs = parseInt(document.getElementById('runsInput')?.value || 1);

        // Get current blueprint type ID from the page
        const bpTypeId = window.BLUEPRINT_DATA?.type_id || getCurrentBlueprintTypeId();

        if (!bpTypeId) {
            console.error('Cannot determine blueprint type ID for recalculation');
            return false;
        }

        // Build URL with current Configure values
        // Start with a clean URL (only keep certain params)
        const cleanUrl = new URL(window.location.pathname, window.location.origin);

        // Copy over params we want to keep
        const paramsToKeep = ['buy', 'next', 'sim', 'draft'];
        const originalUrl = new URL(window.location.href);
        for (const param of paramsToKeep) {
            const value = originalUrl.searchParams.get(param);
            if (value) {
                cleanUrl.searchParams.set(param, value);
            }
        }

        // Set ME/TE for main blueprint (these are REQUIRED)
        cleanUrl.searchParams.set('runs', runs);
        cleanUrl.searchParams.set('me', config.mainME || 0);
        cleanUrl.searchParams.set('te', config.mainTE || 0);
        craftBPDebugLog(`Setting main blueprint: me=${config.mainME || 0}, te=${config.mainTE || 0}`);

        // Set ME/TE for all blueprints
        craftBPDebugLog(`Applying ME/TE for ${Object.keys(config.blueprintConfigs).length} blueprints`);
        const useBlueprintTypeIds = [];
        for (const [typeId, bpConfig] of Object.entries(config.blueprintConfigs)) {
            if (bpConfig.me !== undefined) {
                cleanUrl.searchParams.set(`me_${typeId}`, bpConfig.me);
                craftBPDebugLog(`Setting me_${typeId}=${bpConfig.me}`);
            }
            if (bpConfig.te !== undefined) {
                cleanUrl.searchParams.set(`te_${typeId}`, bpConfig.te);
                craftBPDebugLog(`Setting te_${typeId}=${bpConfig.te}`);
            }
            if (bpConfig.use === true || bpConfig.use === 1 || bpConfig.use === '1') {
                const numericTypeId = Number(typeId) || 0;
                if (numericTypeId > 0) {
                    useBlueprintTypeIds.push(numericTypeId);
                }
            }
        }

        if (useBlueprintTypeIds.length > 0) {
            cleanUrl.searchParams.set('use_bp', useBlueprintTypeIds.join(','));
        }

        const buildEnvironment = config.buildEnvironment || {};
        if (buildEnvironment.system) {
            cleanUrl.searchParams.set('build_system', buildEnvironment.system);
        }
        if (Number.isFinite(buildEnvironment.systemId) && buildEnvironment.systemId > 0) {
            cleanUrl.searchParams.set('build_system_id', buildEnvironment.systemId);
        }
        if (Number.isFinite(buildEnvironment.systemIndex) && buildEnvironment.systemIndex > 0) {
            cleanUrl.searchParams.set('build_system_index', buildEnvironment.systemIndex);
        }
        if (buildEnvironment.structureType && buildEnvironment.structureType !== 'none') {
            cleanUrl.searchParams.set('build_structure', buildEnvironment.structureType);
        }
        if (Number.isFinite(buildEnvironment.structureId) && buildEnvironment.structureId > 0) {
            cleanUrl.searchParams.set('build_structure_id', buildEnvironment.structureId);
        }
        if (Array.isArray(buildEnvironment.rigKeys) && buildEnvironment.rigKeys.length > 0) {
            cleanUrl.searchParams.set('build_rigs', buildEnvironment.rigKeys.join(','));
        }

        const industryFee = config.industryFee || {};
        cleanUrl.searchParams.set('industry_fee_enabled', industryFee.enabled ? '1' : '0');
        if (industryFee.structure_type_id) {
            cleanUrl.searchParams.set('industry_fee_structure_type_id', industryFee.structure_type_id);
        }
        if (industryFee.security) {
            cleanUrl.searchParams.set('industry_fee_security', industryFee.security);
        }
        if (industryFee.system_id) {
            cleanUrl.searchParams.set('industry_fee_system_id', industryFee.system_id);
        }
        if (industryFee.manufacturing_cost !== null && industryFee.manufacturing_cost !== undefined) {
            cleanUrl.searchParams.set('industry_fee_manufacturing_cost', industryFee.manufacturing_cost);
        }
        if (industryFee.invention_cost !== null && industryFee.invention_cost !== undefined) {
            cleanUrl.searchParams.set('industry_fee_invention_cost', industryFee.invention_cost);
        }
        if (industryFee.copying_cost !== null && industryFee.copying_cost !== undefined) {
            cleanUrl.searchParams.set('industry_fee_copying_cost', industryFee.copying_cost);
        }
        if (industryFee.reaction_cost !== null && industryFee.reaction_cost !== undefined) {
            cleanUrl.searchParams.set('industry_fee_reaction_cost', industryFee.reaction_cost);
        }
        if (industryFee.facility_tax !== null && industryFee.facility_tax !== undefined) {
            cleanUrl.searchParams.set('industry_fee_facility_tax', industryFee.facility_tax);
        }
        if (industryFee.system_cost_bonus !== null && industryFee.system_cost_bonus !== undefined) {
            cleanUrl.searchParams.set('industry_fee_system_cost_bonus', industryFee.system_cost_bonus);
        }
        if (Array.isArray(industryFee.rig_ids) && industryFee.rig_ids.length > 0) {
            cleanUrl.searchParams.set('industry_fee_rig_ids', industryFee.rig_ids.join(','));
        }
        cleanUrl.searchParams.set('industry_fee_alpha', industryFee.alpha ? '1' : '0');
        if (industryFee.extra_params) {
            cleanUrl.searchParams.set('industry_fee_extra_params', industryFee.extra_params);
        }

        craftBPDebugLog(`Reloading with URL: ${cleanUrl.toString()}`);

        // Keep the target tab (where user is switching to)
        const targetTab = window.craftBPFlags.switchingToTab || 'materials';
        cleanUrl.searchParams.set('active_tab', targetTab);

        // Reset the flag
        window.craftBPFlags.hasPendingMETEChanges = false;

        // Navigate to new URL with updated parameters
        window.location.href = cleanUrl.toString();
        return true; // Page will reload

    } catch (error) {
        console.error('Error applying Configure changes:', error);
        return false;
    }
}

/**
 * Get current ME/TE configuration from Config tab
 */
function getCurrentMETEConfig() {
    const config = {
        mainME: 0,
        mainTE: 0,
        blueprintConfigs: {},
        buildEnvironment: {},
        industryFee: {}
    };

    // Get ME/TE inputs from config tab
    const meTeInputs = document.querySelectorAll('#configure-pane input[name^="me_"], #configure-pane input[name^="te_"]');
    const useBpInputs = document.querySelectorAll('#configure-pane input.bp-use-input[data-blueprint-type-id]');

    craftBPDebugLog(`getCurrentMETEConfig: Found ${meTeInputs.length} ME/TE inputs and ${useBpInputs.length} Use-BP toggles`);

    meTeInputs.forEach(input => {
        const name = input.name;
        const value = parseInt(input.value) || 0;

        craftBPDebugLog(`Input ${name} = ${value}`);

        if (name.startsWith('me_')) {
            const typeId = name.replace('me_', '');
            if (!config.blueprintConfigs[typeId]) {
                config.blueprintConfigs[typeId] = {};
            }
            config.blueprintConfigs[typeId].me = Math.max(0, Math.min(value, 10));

            // If this is the main blueprint, store it separately
            const currentBpId = getCurrentBlueprintTypeId();
            if (parseInt(typeId) === parseInt(currentBpId)) {
                config.mainME = config.blueprintConfigs[typeId].me;
                craftBPDebugLog(`Detected main blueprint ME: ${config.mainME}`);
            }
        } else if (name.startsWith('te_')) {
            const typeId = name.replace('te_', '');
            if (!config.blueprintConfigs[typeId]) {
                config.blueprintConfigs[typeId] = {};
            }
            config.blueprintConfigs[typeId].te = Math.max(0, Math.min(value, 20));

            // If this is the main blueprint, store it separately
            const currentBpId = getCurrentBlueprintTypeId();
            if (parseInt(typeId) === parseInt(currentBpId)) {
                config.mainTE = config.blueprintConfigs[typeId].te;
                craftBPDebugLog(`Detected main blueprint TE: ${config.mainTE}`);
            }
        }
    });

    useBpInputs.forEach((input) => {
        const blueprintTypeId = String(Number(input.getAttribute('data-blueprint-type-id')) || '');
        if (!blueprintTypeId) {
            return;
        }
        if (!config.blueprintConfigs[blueprintTypeId]) {
            config.blueprintConfigs[blueprintTypeId] = {};
        }
        config.blueprintConfigs[blueprintTypeId].use = (!input.disabled && input.checked) ? 1 : 0;
    });

    config.buildEnvironment = getBuildEnvironmentFromDom();
    config.industryFee = getIndustryFeeConfigFromDom();

    return config;
}

/**
 * Get current blueprint type ID from the page
 */
function getCurrentBlueprintTypeId() {
    // First try to get from page data
    if (window.BLUEPRINT_DATA?.bp_type_id) {
        return window.BLUEPRINT_DATA.bp_type_id;
    }

    // Try to get from URL path
    const pathMatch = window.location.pathname.match(/\/craft\/(\d+)\//);
    if (pathMatch) {
        return pathMatch[1];
    }

    // Fallback: try to get from page data (legacy)
    return window.BLUEPRINT_DATA?.type_id;
}

/**
 * Show loading indicator during recalculation
 */
function showLoadingIndicator() {
    // Add loading overlay or spinner
    const indicator = document.createElement('div');
    indicator.id = 'craft-bp-loading';
    indicator.innerHTML = `
        <div class="d-flex justify-content-center align-items-center position-fixed top-0 start-0 w-100 h-100"
            style="background: rgba(0,0,0,0.7); z-index: 9999;">
            <div class="bg-white rounded p-4 text-center">
                <div class="spinner-border text-primary mb-3" role="status">
                    <span class="visually-hidden">${__('Loading...')}</span>
                </div>
                <p class="mb-0">${__('Recalculating with new ME/TE values...')}</p>
            </div>
        </div>
    `;
    document.body.appendChild(indicator);
}

/**
 * Hide loading indicator
 */
function hideLoadingIndicator() {
    const indicator = document.getElementById('craft-bp-loading');
    if (indicator) {
        indicator.remove();
    }
}

function getIndustryFeeApiUrl() {
    return String(window.BLUEPRINT_DATA?.urls?.craft_industry_fees || '').trim();
}

function collectIndustryFeeJobs() {
    const jobsByProductType = new Map();
    const addJob = (productTypeId, runs) => {
        const numericProductTypeId = Number(productTypeId) || 0;
        const numericRuns = Math.max(0, Math.ceil(Number(runs) || 0));
        if (!numericProductTypeId || numericRuns <= 0) {
            return;
        }
        jobsByProductType.set(
            numericProductTypeId,
            (jobsByProductType.get(numericProductTypeId) || 0) + numericRuns
        );
    };

    // Main product manufacturing job.
    const mainProductTypeId = getProductTypeIdValue();
    const mainRuns = Number(document.getElementById('runsInput')?.value || window.BLUEPRINT_DATA?.num_runs || 0) || 0;
    addJob(mainProductTypeId, mainRuns);

    // Produced sub-components based on current Buy/Prod switches.
    const cycles = (window.SimulationAPI && typeof window.SimulationAPI.getProductionCycles === 'function')
        ? (window.SimulationAPI.getProductionCycles() || [])
        : [];
    cycles.forEach((entry) => {
        const typeId = Number(entry?.typeId || entry?.type_id || 0) || 0;
        const runs = Number(entry?.cycles || 0) || 0;
        addJob(typeId, runs);
    });

    return Array.from(jobsByProductType.entries())
        .map(([product_id, runs]) => ({ product_id, runs }))
        .sort((left, right) => Number(left.product_id) - Number(right.product_id));
}

function buildIndustryFeeSignature(industryFeeConfig, jobs) {
    return JSON.stringify({
        cfg: industryFeeConfig || {},
        jobs: Array.isArray(jobs) ? jobs : [],
    });
}

function getIndustryFeeTotalCost() {
    return Math.max(0, Number(CRAFT_INDUSTRY_FEE_STATE.totalJobCost) || 0);
}

async function ensureIndustryFeeEstimateUpToDate() {
    const feeConfig = getIndustryFeeConfigFromDom();
    const jobs = collectIndustryFeeJobs();
    const signature = buildIndustryFeeSignature(feeConfig, jobs);

    if (!feeConfig.enabled || jobs.length === 0) {
        CRAFT_INDUSTRY_FEE_STATE.signature = signature;
        CRAFT_INDUSTRY_FEE_STATE.loaded = true;
        CRAFT_INDUSTRY_FEE_STATE.loading = false;
        CRAFT_INDUSTRY_FEE_STATE.totalJobCost = 0;
        CRAFT_INDUSTRY_FEE_STATE.totalApiCost = 0;
        CRAFT_INDUSTRY_FEE_STATE.jobs = [];
        CRAFT_INDUSTRY_FEE_STATE.errors = [];
        return;
    }

    const endpoint = getIndustryFeeApiUrl();
    if (!endpoint) {
        return;
    }
    if (CRAFT_INDUSTRY_FEE_STATE.loading && CRAFT_INDUSTRY_FEE_STATE.signature === signature) {
        return;
    }
    if (CRAFT_INDUSTRY_FEE_STATE.loaded && CRAFT_INDUSTRY_FEE_STATE.signature === signature) {
        return;
    }

    CRAFT_INDUSTRY_FEE_STATE.loading = true;
    CRAFT_INDUSTRY_FEE_STATE.signature = signature;

    const params = new URLSearchParams();
    params.set(
        'jobs',
        jobs.map((job) => `${job.product_id}:${job.runs}`).join(',')
    );
    if (feeConfig.structure_type_id) {
        params.set('structure_type_id', String(Math.floor(Number(feeConfig.structure_type_id))));
    }
    if (feeConfig.security) {
        params.set('security', String(feeConfig.security).toUpperCase());
    }
    if (feeConfig.system_id) {
        params.set('system_id', String(Math.floor(Number(feeConfig.system_id))));
    }
    if (feeConfig.manufacturing_cost !== null && feeConfig.manufacturing_cost !== undefined) {
        params.set('manufacturing_cost', String(feeConfig.manufacturing_cost));
    }
    if (feeConfig.invention_cost !== null && feeConfig.invention_cost !== undefined) {
        params.set('invention_cost', String(feeConfig.invention_cost));
    }
    if (feeConfig.copying_cost !== null && feeConfig.copying_cost !== undefined) {
        params.set('copying_cost', String(feeConfig.copying_cost));
    }
    if (feeConfig.reaction_cost !== null && feeConfig.reaction_cost !== undefined) {
        params.set('reaction_cost', String(feeConfig.reaction_cost));
    }
    if (feeConfig.facility_tax !== null && feeConfig.facility_tax !== undefined) {
        params.set('facility_tax', String(feeConfig.facility_tax));
    }
    if (feeConfig.system_cost_bonus !== null && feeConfig.system_cost_bonus !== undefined) {
        params.set('system_cost_bonus', String(feeConfig.system_cost_bonus));
    }
    if (feeConfig.alpha) {
        params.set('alpha', 'true');
    }
    if (Array.isArray(feeConfig.rig_ids)) {
        feeConfig.rig_ids.forEach((rigId) => {
            const numericRigId = Number(rigId) || 0;
            if (numericRigId > 0) {
                params.append('rig_id', String(Math.floor(numericRigId)));
            }
        });
    }
    if (feeConfig.extra_params) {
        params.set('extra_params', String(feeConfig.extra_params).trim());
    }

    try {
        const requestUrl = `${endpoint}?${params.toString()}`;
        const response = await fetch(requestUrl, {
            method: 'GET',
            credentials: 'same-origin',
        });
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        const payload = await response.json();
        CRAFT_INDUSTRY_FEE_STATE.loaded = true;
        CRAFT_INDUSTRY_FEE_STATE.totalJobCost = Math.max(0, Number(payload?.total_job_cost) || 0);
        CRAFT_INDUSTRY_FEE_STATE.totalApiCost = Math.max(0, Number(payload?.total_api_cost) || 0);
        CRAFT_INDUSTRY_FEE_STATE.jobs = Array.isArray(payload?.jobs) ? payload.jobs : [];
        CRAFT_INDUSTRY_FEE_STATE.errors = Array.isArray(payload?.errors) ? payload.errors : [];
        if (typeof recalcFinancials === 'function') {
            recalcFinancials();
        }
    } catch (error) {
        console.error('[CraftBP] Failed loading industry fee estimate', error);
        CRAFT_INDUSTRY_FEE_STATE.loaded = true;
        CRAFT_INDUSTRY_FEE_STATE.totalJobCost = 0;
        CRAFT_INDUSTRY_FEE_STATE.totalApiCost = 0;
        CRAFT_INDUSTRY_FEE_STATE.jobs = [];
        CRAFT_INDUSTRY_FEE_STATE.errors = [{ error: String(error && error.message ? error.message : error) }];
        if (typeof recalcFinancials === 'function') {
            recalcFinancials();
        }
    } finally {
        CRAFT_INDUSTRY_FEE_STATE.loading = false;
    }
}

/**
 * Format a number as a price with ISK suffix
 * @param {number} num - The number to format
 * @returns {string} Formatted price string
 */
function formatPrice(num) {
    return num.toLocaleString('de-DE', {minimumFractionDigits: 2, maximumFractionDigits: 2}) + ' ISK';
}

/**
 * Format a number with thousand separators
 * @param {number} num - The number to format
 * @returns {string} Formatted number string
 */
function formatNumber(num) {
    return num.toLocaleString('de-DE', {minimumFractionDigits: 2, maximumFractionDigits: 2});
}

/**
 * Recalculate financial totals
 */
function recalcFinancials() {
    if (typeof ensureIndustryFeeEstimateUpToDate === 'function') {
        ensureIndustryFeeEstimateUpToDate().catch((error) => {
            console.error('[CraftBP] Failed scheduling industry fee estimate refresh', error);
        });
    }

    let costTotal = 0;
    let revTotal = 0;

    document.querySelectorAll('#financialItemsBody tr').forEach(tr => {
        const qtyCell = tr.querySelector('[data-qty]');
        if (!qtyCell) {
            return;
        }

        let rawQty = null;
        if (typeof qtyCell.getAttribute === 'function') {
            rawQty = qtyCell.getAttribute('data-qty');
        }
        if ((rawQty === null || rawQty === undefined || rawQty === '') && qtyCell.dataset) {
            rawQty = qtyCell.dataset.qty;
        }
        if (rawQty === null || rawQty === undefined || rawQty === '') {
            return;
        }

        const qty = Math.max(0, Math.ceil(parseFloat(rawQty))) || 0;
        const costInput = tr.querySelector('.real-price');
        const revInput = tr.querySelector('.sale-price-unit');

        if (costInput) {
            const typeId = Number(tr.getAttribute('data-type-id')) || 0;
            let unitCost = parseFloat(costInput.value) || 0;

            // If real price is 0, fall back to fuzzwork.
            if (unitCost <= 0) {
                if (window.SimulationAPI && typeof window.SimulationAPI.getPrice === 'function' && typeId) {
                    const info = window.SimulationAPI.getPrice(typeId, 'buy');
                    unitCost = info && typeof info.value === 'number' ? info.value : 0;
                } else {
                    const fuzzInp = tr.querySelector('.fuzzwork-price');
                    unitCost = parseFloat(fuzzInp ? fuzzInp.value : 0) || 0;
                }
            }

            const cost = unitCost * qty;
            const totalCostEl = tr.querySelector('.total-cost');
            if (totalCostEl) {
                totalCostEl.textContent = formatPrice(cost);
            }
            costTotal += cost;
        }

        if (revInput) {
            const rev = (parseFloat(revInput.value) || 0) * qty;
            const totalRevenueEl = tr.querySelector('.total-revenue');
            if (totalRevenueEl) {
                totalRevenueEl.textContent = formatPrice(rev);
            }
            revTotal += rev;
        }
    });

    // Credit any craft-cycle surplus (extra produced due to cycle rounding).
    // IMPORTANT: This must depend on the current Buy/Prod switches.
    // We therefore compute cycles from SimulationAPI state when available.
    let surplusRevenue = 0;
    try {
        const productTypeId = Number(CRAFT_BP.productTypeId) || 0;

        if (window.SimulationAPI && typeof window.SimulationAPI.getPrice === 'function') {
            const cycles = (typeof window.SimulationAPI.getProductionCycles === 'function')
                ? window.SimulationAPI.getProductionCycles()
                : [];

            if (Array.isArray(cycles) && cycles.length) {
                cycles.forEach(entry => {
                    const typeId = Number(entry.typeId || entry.type_id || 0) || 0;
                    const surplusQty = Number(entry.surplus) || 0;
                    if (!typeId || surplusQty <= 0) return;
                    if (productTypeId && typeId === productTypeId) return;

                    const priceInfo = window.SimulationAPI.getPrice(typeId, 'sale');
                    const unitPrice = priceInfo && typeof priceInfo.value === 'number' ? priceInfo.value : 0;
                    if (unitPrice > 0) {
                        surplusRevenue += unitPrice * surplusQty;
                    }
                });
            } else {
                // Fallback for older payloads (static). Note: does NOT reflect switch state.
                const cyclesSummary = window.BLUEPRINT_DATA?.craft_cycles_summary || {};
                Object.keys(cyclesSummary).forEach(key => {
                    const entry = cyclesSummary[key] || {};
                    const typeId = Number(entry.type_id || key) || 0;
                    const surplusQty = Number(entry.surplus) || 0;
                    if (!typeId || surplusQty <= 0) return;
                    if (productTypeId && typeId === productTypeId) return;

                    const priceInfo = window.SimulationAPI.getPrice(typeId, 'sale');
                    const unitPrice = priceInfo && typeof priceInfo.value === 'number' ? priceInfo.value : 0;
                    if (unitPrice > 0) {
                        surplusRevenue += unitPrice * surplusQty;
                    }
                });
            }
        }
    } catch (e) {
        console.warn('Unable to compute surplus revenue credit:', e);
    }

    const surplusWrapperEl = document.getElementById('financialSurplusWrapper');
    const surplusValueEl = document.getElementById('financialSummarySurplus');
    if (surplusValueEl) {
        surplusValueEl.textContent = formatPrice(surplusRevenue);
    }
    if (surplusWrapperEl) {
        surplusWrapperEl.classList.toggle('d-none', !(surplusRevenue > 0));
    }

    revTotal += surplusRevenue;

    const industryFeeTotal = getIndustryFeeTotalCost();
    const costTotalWithFees = costTotal + industryFeeTotal;
    const profit = revTotal - costTotalWithFees;
    // Margin = profit / revenue (not markup on cost).
    const marginValue = revTotal > 0 ? (profit / revTotal) * 100 : 0;
    const marginText = marginValue.toFixed(1);

    const grandTotalIndustryFeeEl = document.querySelector('.grand-total-industry-fee');
    const grandTotalCostEl = document.querySelector('.grand-total-cost');
    const grandTotalRevEl = document.querySelector('.grand-total-rev');
    const profitEl = document.querySelector('.profit');
    const profitPctEl = document.querySelector('.profit-pct');

    if (grandTotalIndustryFeeEl) {
        grandTotalIndustryFeeEl.textContent = formatPrice(industryFeeTotal);
    }

    if (grandTotalCostEl) {
        grandTotalCostEl.textContent = formatPrice(costTotal);
    }

    if (grandTotalRevEl) {
        grandTotalRevEl.textContent = formatPrice(revTotal);
    }

    if (profitEl && profitEl.childNodes.length > 0) {
        profitEl.childNodes[0].textContent = formatPrice(profit) + ' ';
        if (profitPctEl) {
            profitPctEl.textContent = `(${marginText}%)`;
        }
    }

    const summaryCostEl = document.getElementById('financialSummaryCost');
    if (summaryCostEl) {
        summaryCostEl.textContent = formatPrice(costTotalWithFees);
    }

    const summaryRevenueEl = document.getElementById('financialSummaryRevenue');
    if (summaryRevenueEl) {
        summaryRevenueEl.textContent = formatPrice(revTotal);
    }

    const summaryProfitEl = document.getElementById('financialSummaryProfit');
    if (summaryProfitEl) {
        summaryProfitEl.textContent = formatPrice(profit);
        summaryProfitEl.classList.remove('text-success', 'text-danger');
        summaryProfitEl.classList.add(profit >= 0 ? 'text-success' : 'text-danger');
    }

    const summaryIndustryFeesEl = document.getElementById('financialSummaryIndustryFees');
    if (summaryIndustryFeesEl) {
        summaryIndustryFeesEl.textContent = formatPrice(industryFeeTotal);
    }

    const summaryMarginEl = document.getElementById('financialSummaryMargin');
    if (summaryMarginEl) {
        summaryMarginEl.textContent = `${marginText}%`;
        summaryMarginEl.classList.remove('bg-success-subtle', 'text-success-emphasis', 'bg-danger-subtle', 'text-danger-emphasis');
        if (profit >= 0) {
            summaryMarginEl.classList.add('bg-success-subtle', 'text-success-emphasis');
        } else {
            summaryMarginEl.classList.add('bg-danger-subtle', 'text-danger-emphasis');
        }
    }

    const summaryUpdatedEl = document.getElementById('financialSummaryUpdated');
    const heroProfitEl = document.getElementById('heroProfit');
    const heroMarginEl = document.getElementById('heroMargin');
    const heroUpdatedEl = document.getElementById('heroUpdated');
    const quickProfitEl = document.getElementById('quickProfit');
    const quickMarginEl = document.getElementById('quickMargin');

    if (heroProfitEl) {
        heroProfitEl.textContent = formatPrice(profit);
        const profitCard = heroProfitEl.closest('.hero-kpi');
        if (profitCard) {
            profitCard.classList.toggle('negative', profit < 0);
            profitCard.classList.toggle('positive', profit >= 0);
        }
    }

    if (quickProfitEl) {
        quickProfitEl.textContent = formatPrice(profit);
        quickProfitEl.classList.remove('text-success', 'text-danger');
        quickProfitEl.classList.add(profit >= 0 ? 'text-success' : 'text-danger');
    }

    if (heroMarginEl) {
        heroMarginEl.textContent = `${marginText}%`;
        const marginCard = heroMarginEl.closest('.hero-kpi');
        if (marginCard) {
            marginCard.classList.toggle('negative', marginValue < 0);
            marginCard.classList.toggle('positive', marginValue >= 0);
        }
    }

    if (quickMarginEl) {
        quickMarginEl.textContent = `${marginText}%`;
    }

    const now = new Date();
    const formattedTime = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

    if (summaryUpdatedEl) {
        summaryUpdatedEl.textContent = formattedTime;
        summaryUpdatedEl.setAttribute('title', now.toLocaleString());
    }

    if (heroUpdatedEl) {
        heroUpdatedEl.textContent = formattedTime;
        heroUpdatedEl.setAttribute('title', now.toLocaleString());
    }
}

/**
 * Batch fetch prices from Fuzzwork API
 * @param {Array} typeIds - Array of EVE type IDs
 * @returns {Promise<Object>} Promise resolving to price data
 */
async function fetchAllPrices(typeIds) {
    const ids = Array.isArray(typeIds) ? typeIds : [];
    const numericIds = ids
        .map(id => String(id).trim())
        .filter(Boolean)
        .filter(id => /^\d+$/.test(id));
    const uniqueTypeIds = [...new Set(numericIds)];

    if (uniqueTypeIds.length === 0) {
        console.warn('fetchAllPrices called without valid type IDs');
        return {};
    }

    if (!CRAFT_BP.fuzzworkUrl) {
        const fallbackUrl = window.BLUEPRINT_DATA?.fuzzwork_price_url;
        if (fallbackUrl) {
            CRAFT_BP.fuzzworkUrl = fallbackUrl;
        }
    }

    const baseUrl = CRAFT_BP.fuzzworkUrl;
    if (!baseUrl) {
        console.error('No Fuzzwork URL configured; skipping price fetch.');
        return {};
    }

    const separator = baseUrl.includes('?') ? '&' : '?';
    const requestUrl = `${baseUrl}${separator}type_id=${uniqueTypeIds.join(',')}`;

    try {
        craftBPDebugLog('[CraftBP] Loading Fuzzwork prices from', requestUrl);
        const resp = await fetch(requestUrl, { credentials: 'same-origin' });
        if (!resp.ok) {
            console.error('Fuzzwork price request failed:', resp.status, resp.statusText);
            try {
                const errorPayload = await resp.json();
                console.error('Fuzzwork response body:', errorPayload);
            } catch (jsonErr) {
                console.error('Unable to parse error response JSON', jsonErr);
            }
            return {};
        }
        const data = await resp.json();
        craftBPDebugLog('[CraftBP] Fuzzwork prices received', data);
        return data;
    } catch (e) {
        console.error('Error fetching prices from Fuzzwork, URL:', requestUrl, e);
        return {};
    }
}

async function fetchFuzzworkAggregates(typeIds) {
    const ids = Array.isArray(typeIds) ? typeIds : [];
    const numericIds = ids
        .map(id => String(id).trim())
        .filter(Boolean)
        .filter(id => /^\d+$/.test(id));
    const uniqueTypeIds = [...new Set(numericIds)];

    if (uniqueTypeIds.length === 0) {
        return {};
    }

    if (!CRAFT_BP.fuzzworkUrl) {
        const fallbackUrl = window.BLUEPRINT_DATA?.fuzzwork_price_url;
        if (fallbackUrl) {
            CRAFT_BP.fuzzworkUrl = fallbackUrl;
        }
    }

    const baseUrl = CRAFT_BP.fuzzworkUrl;
    if (!baseUrl) {
        console.error('No Fuzzwork URL configured; skipping aggregates fetch.');
        return {};
    }

    const separator = baseUrl.includes('?') ? '&' : '?';
    const requestUrl = `${baseUrl}${separator}type_id=${uniqueTypeIds.join(',')}&full=1`;

    try {
        craftBPDebugLog('[CraftBP] Loading Fuzzwork aggregates from', requestUrl);
        const resp = await fetch(requestUrl, { credentials: 'same-origin' });
        if (!resp.ok) {
            console.error('Fuzzwork aggregates request failed:', resp.status, resp.statusText);
            return {};
        }
        const data = await resp.json();
        craftBPDebugLog('[CraftBP] Fuzzwork aggregates received', data);
        return data && typeof data === 'object' ? data : {};
    } catch (e) {
        console.error('Error fetching aggregates from Fuzzwork, URL:', requestUrl, e);
        return {};
    }
}

function flattenFuzzworkEntry(entry) {
    const flat = {};
    if (!entry || typeof entry !== 'object') return flat;

    Object.keys(entry).forEach(k => {
        const v = entry[k];
        if (v && typeof v === 'object' && !Array.isArray(v)) {
            Object.keys(v).forEach(sub => {
                flat[`${k}.${sub}`] = v[sub];
            });
        } else {
            flat[k] = v;
        }
    });

    return flat;
}

function sortFuzzworkColumnKeys(keys) {
    const groupOrder = ['buy', 'sell'];
    const subOrder = ['volume', 'min', 'max', 'avg', 'median', 'percentile', 'wavg', 'stddev'];

    function keyRank(k) {
        const parts = String(k).split('.');
        const group = parts[0] || '';
        const sub = parts[1] || '';

        const gIdx = groupOrder.includes(group) ? groupOrder.indexOf(group) : groupOrder.length;
        const sIdx = subOrder.includes(sub) ? subOrder.indexOf(sub) : subOrder.length;

        return { gIdx, sIdx, group, sub, full: k };
    }

    return [...keys]
        .map(k => ({ k, r: keyRank(k) }))
        .sort((a, b) => {
            if (a.r.gIdx !== b.r.gIdx) return a.r.gIdx - b.r.gIdx;
            if (a.r.group !== b.r.group) return a.r.group.localeCompare(b.r.group);
            if (a.r.sIdx !== b.r.sIdx) return a.r.sIdx - b.r.sIdx;
            if (a.r.sub !== b.r.sub) return a.r.sub.localeCompare(b.r.sub);
            return a.r.full.localeCompare(b.r.full);
        })
        .map(x => x.k);
}

/**
 * Populate price inputs with fetched data
 * @param {Array} allInputs - Array of input elements
 * @param {Object} prices - Price data from API
 */
function populatePrices(allInputs, prices) {
    // Populate all material and sale price inputs
    allInputs.forEach(inp => {
        const tid = inp.getAttribute('data-type-id');
        const raw = prices[tid] ?? prices[String(parseInt(tid, 10))];
        let price = raw != null ? parseFloat(raw) : NaN;
        if (isNaN(price)) price = 0;

        inp.value = price.toFixed(2);

        if (window.SimulationAPI && typeof window.SimulationAPI.setPrice === 'function') {
            window.SimulationAPI.setPrice(tid, 'fuzzwork', price);
        }

        if (price <= 0) {
            inp.classList.add('bg-warning', 'border-warning');
            inp.setAttribute('title', __('Price not available (Fuzzwork)'));
        } else {
            inp.classList.remove('bg-warning', 'border-warning');
            inp.removeAttribute('title');
        }
    });

    // Override final product sale price using its true type_id
    if (CRAFT_BP.productTypeId) {
        const finalKey = String(CRAFT_BP.productTypeId);
        const rawFinal = prices[finalKey] ?? prices[String(parseInt(finalKey, 10))];
        let finalPrice = rawFinal != null ? parseFloat(rawFinal) : NaN;
        if (isNaN(finalPrice)) finalPrice = 0;

        const saleSelector = `.sale-price-unit[data-type-id="${finalKey}"]`;
        const saleInput = document.querySelector(saleSelector);
        if (saleInput) {
            if (saleInput.dataset.userModified !== 'true') {
                saleInput.value = finalPrice.toFixed(2);
                updatePriceInputManualState(saleInput, false);
            }
            if (finalPrice <= 0) {
                saleInput.classList.add('bg-warning', 'border-warning');
                saleInput.setAttribute('title', __('Price not available (Fuzzwork)'));
            } else {
                saleInput.classList.remove('bg-warning', 'border-warning');
                saleInput.removeAttribute('title');
            }
        }

        if (window.SimulationAPI && typeof window.SimulationAPI.setPrice === 'function') {
            window.SimulationAPI.setPrice(CRAFT_BP.productTypeId, 'sale', finalPrice);
        }
    }
}

function buildFinancialRow(item, pricesMap) {
    const rowKind = String(item.rowKind || 'material').toLowerCase() === 'bpc' ? 'bpc' : 'material';
    const rowKey = String(item.rowKey || `${rowKind}:${item.typeId}`);
    const isBpc = rowKind === 'bpc';
    const imagePath = isBpc ? 'bp' : 'icon';
    const itemTypeName = String(item.typeName || item.name || item.type_id || item.typeId || '');
    const rowTagHtml = isBpc
        ? `<span class="badge bg-warning-subtle text-warning-emphasis border border-warning-subtle ms-2 craft-row-kind-marker">${escapeHtml(__('BPC'))}</span>`
        : '<span class="craft-row-kind-marker d-none"></span>';
    const qtyBadgeClass = isBpc ? 'bg-warning text-dark' : 'bg-primary text-white';
    const fixedUnitPrice = Number(item.unitPrice);
    const hasFixedUnitPrice = Number.isFinite(fixedUnitPrice) && fixedUnitPrice > 0;

    const row = document.createElement('tr');
    row.setAttribute('data-type-id', String(item.typeId));
    row.setAttribute('data-row-kind', rowKind);
    row.setAttribute('data-row-key', rowKey);
    row.classList.toggle('table-warning', isBpc);

    row.innerHTML = `
        <td class="fw-semibold">
            <div class="d-flex align-items-center gap-3 craft-planner-item-flex">
                <img src="https://images.evetech.net/types/${item.typeId}/${imagePath}?size=32" alt="${escapeHtml(itemTypeName)}" class="rounded" style="width:28px;height:28px;background:#f3f4f6;" onerror="this.style.display='none';">
                <span class="craft-planner-item-name-wrap">
                    <span class="badge bg-info-subtle text-info-emphasis px-2 py-1 craft-planner-item-name">${escapeHtml(itemTypeName)}</span>${rowTagHtml}
                </span>
            </div>
        </td>
        <td class="text-end">
            <span class="badge ${qtyBadgeClass}" data-qty="${item.quantity}">${formatInteger(item.quantity)}</span>
        </td>
        <td class="text-end">
            <input type="number" min="0" step="0.01" class="form-control form-control-sm fuzzwork-price text-end bg-light" data-type-id="${item.typeId}" value="0" readonly>
        </td>
        <td class="text-end">
            <input type="number" min="0" step="0.01" class="form-control form-control-sm real-price text-end" data-type-id="${item.typeId}" value="0">
        </td>
        <td class="text-end total-cost">0</td>
    `;

    const fuzzInput = row.querySelector('.fuzzwork-price');
    const realInput = row.querySelector('.real-price');

    const priceEntry = pricesMap.get(item.typeId) || {};
    const fuzzPrice = Number(priceEntry.fuzzwork || 0);
    const realPrice = Number(priceEntry.real || 0);

    fuzzInput.value = fuzzPrice.toFixed(2);
    if (fuzzPrice <= 0) {
        fuzzInput.classList.add('bg-warning', 'border-warning');
        fuzzInput.setAttribute('title', __('Price not available (Fuzzwork)'));
    } else {
        fuzzInput.classList.remove('bg-warning', 'border-warning');
        fuzzInput.removeAttribute('title');
    }

    if (realPrice > 0) {
        realInput.value = realPrice.toFixed(2);
        updatePriceInputManualState(realInput, true);
    } else {
        realInput.value = '0.00';
        updatePriceInputManualState(realInput, false);
    }

    if (hasFixedUnitPrice) {
        realInput.value = fixedUnitPrice.toFixed(2);
        realInput.readOnly = true;
        realInput.classList.add('readonly');
        realInput.dataset.fixedPrice = 'true';
        updatePriceInputManualState(realInput, true);
    } else {
        realInput.readOnly = false;
        realInput.classList.remove('readonly');
        realInput.dataset.fixedPrice = 'false';
    }

    if (realInput.dataset.fixedPrice !== 'true') {
        attachPriceInputListener(realInput);
    }

    return { row, typeId: item.typeId, fuzzInput, realInput };
}

function updateFinancialRow(row, item) {
    const rowKind = String(item.rowKind || 'material').toLowerCase() === 'bpc' ? 'bpc' : 'material';
    const rowKey = String(item.rowKey || `${rowKind}:${item.typeId}`);
    const isBpc = rowKind === 'bpc';
    const imagePath = isBpc ? 'bp' : 'icon';
    const itemTypeName = String(item.typeName || item.name || item.type_id || item.typeId || '');
    const fixedUnitPrice = Number(item.unitPrice);
    const hasFixedUnitPrice = Number.isFinite(fixedUnitPrice) && fixedUnitPrice > 0;

    row.setAttribute('data-type-id', String(item.typeId));
    row.setAttribute('data-row-kind', rowKind);
    row.setAttribute('data-row-key', rowKey);
    row.classList.toggle('table-warning', isBpc);

    const nameBadge = row.querySelector('.craft-planner-item-name');
    if (nameBadge) {
        nameBadge.textContent = itemTypeName;
    }

    const rowKindMarker = row.querySelector('.craft-row-kind-marker');
    if (rowKindMarker) {
        if (isBpc) {
            rowKindMarker.className = 'badge bg-warning-subtle text-warning-emphasis border border-warning-subtle ms-2 craft-row-kind-marker';
            rowKindMarker.textContent = __('BPC');
        } else {
            rowKindMarker.className = 'craft-row-kind-marker d-none';
            rowKindMarker.textContent = '';
        }
    }

    const img = row.querySelector('img');
    if (img) {
        img.alt = itemTypeName;
        img.src = `https://images.evetech.net/types/${item.typeId}/${imagePath}?size=32`;
    }

    const qtyBadge = row.querySelector('span.badge[data-qty]');
    if (qtyBadge) {
        qtyBadge.dataset.qty = String(item.quantity);
        qtyBadge.textContent = formatInteger(item.quantity);
        qtyBadge.classList.remove('bg-primary', 'text-white', 'bg-warning', 'text-dark');
        if (isBpc) {
            qtyBadge.classList.add('bg-warning', 'text-dark');
        } else {
            qtyBadge.classList.add('bg-primary', 'text-white');
        }
    }

    const realInput = row.querySelector('.real-price');
    if (realInput) {
        if (hasFixedUnitPrice) {
            realInput.value = fixedUnitPrice.toFixed(2);
            realInput.readOnly = true;
            realInput.classList.add('readonly');
            realInput.dataset.fixedPrice = 'true';
            updatePriceInputManualState(realInput, true);
        } else if (realInput.dataset.fixedPrice === 'true') {
            realInput.readOnly = false;
            realInput.classList.remove('readonly');
            realInput.dataset.fixedPrice = 'false';
        }
    }
}

// Cache the server-rendered dashboard ordering before any JS re-renders the Materials section.
let CRAFT_DASHBOARD_ORDERING_CACHE = null;

function getDashboardMaterialsOrdering() {
    if (CRAFT_DASHBOARD_ORDERING_CACHE) {
        return CRAFT_DASHBOARD_ORDERING_CACHE;
    }

    const container = document.getElementById('materialsGroupsContainer');
    const fallbackGroupName = __('Other');

    const groupOrder = new Map(); // groupName -> index
    const itemOrder = new Map(); // typeId -> { groupIdx, itemIdx }

    if (!container) {
        return { groupOrder, itemOrder, fallbackGroupName };
    }

    // Prefer the original server-rendered markup (.craft-group-card).
    const groupCards = Array.from(container.querySelectorAll('.craft-group-card'));
    if (groupCards.length > 0) {
        groupCards.forEach((card, groupIdx) => {
            const headerSpan = card.querySelector('.craft-group-header > span');
            let groupName = headerSpan && headerSpan.textContent ? headerSpan.textContent.trim() : '';
            if (!groupName) {
                groupName = fallbackGroupName;
            }
            if (!groupOrder.has(groupName)) {
                groupOrder.set(groupName, groupIdx);
            }

            const rows = Array.from(card.querySelectorAll('.craft-item-row[data-type-id]'));
            rows.forEach((row, itemIdx) => {
                const typeId = Number(row.getAttribute('data-type-id')) || 0;
                if (!typeId || itemOrder.has(typeId)) {
                    return;
                }
                itemOrder.set(typeId, { groupIdx, itemIdx });
            });
        });
    } else {
        // Fallback: JS-rendered markup from updateMaterialsTabFromState (bootstrap cards with a table).
        const cards = Array.from(container.querySelectorAll('.card'));
        cards.forEach((card, groupIdx) => {
            const headerLabel = card.querySelector('.card-header span.fw-semibold');
            let groupName = headerLabel && headerLabel.textContent ? headerLabel.textContent.trim() : '';
            if (!groupName) {
                groupName = fallbackGroupName;
            }
            if (!groupOrder.has(groupName)) {
                groupOrder.set(groupName, groupIdx);
            }

            const rows = Array.from(card.querySelectorAll('tbody tr[data-type-id]'));
            rows.forEach((row, itemIdx) => {
                const typeId = Number(row.getAttribute('data-type-id')) || 0;
                if (!typeId || itemOrder.has(typeId)) {
                    return;
                }
                itemOrder.set(typeId, { groupIdx, itemIdx });
            });
        });
    }

    const result = { groupOrder, itemOrder, fallbackGroupName };
    if (groupOrder.size > 0 || itemOrder.size > 0) {
        CRAFT_DASHBOARD_ORDERING_CACHE = result;
    }
    return result;
}

function getFinancialRowKey(item) {
    const rowKind = String(item?.rowKind || 'material').toLowerCase() === 'bpc' ? 'bpc' : 'material';
    const typeId = Number(item?.typeId ?? item?.type_id) || 0;
    if (!typeId) {
        return null;
    }
    return `${rowKind}:${typeId}`;
}

function getBlueprintNameMapFromConfigPane() {
    const map = new Map();
    document.querySelectorAll('#configure-pane .craft-bp-card[data-blueprint-type-id]').forEach((card) => {
        const typeId = Number(card.getAttribute('data-blueprint-type-id')) || 0;
        if (!typeId || map.has(typeId)) {
            return;
        }
        const titleEl = card.querySelector('.card-title');
        const title = String(titleEl && titleEl.textContent ? titleEl.textContent : '').trim();
        if (title) {
            map.set(typeId, title);
        }
    });
    return map;
}

function getBlueprintUseSelectionFromDom() {
    const selections = new Map();
    document.querySelectorAll('#configure-pane input.bp-use-input[data-blueprint-type-id]').forEach((input) => {
        const blueprintTypeId = Number(input.getAttribute('data-blueprint-type-id')) || 0;
        if (!blueprintTypeId) {
            return;
        }
        const selected = !input.disabled && Boolean(input.checked);
        selections.set(blueprintTypeId, selected);
    });
    return selections;
}

function buildBlueprintUsageContextByProductType() {
    const payload = window.BLUEPRINT_DATA || {};
    const blueprintConfigs = getBlueprintConfigsForFinancialPlanner();
    const blueprintNames = getBlueprintNameMapFromConfigPane();
    const selectedByBlueprintType = getBlueprintUseSelectionFromDom();
    const byProductType = new Map();

    blueprintConfigs.forEach((bp) => {
        const blueprintTypeId = Number(bp?.type_id || bp?.typeId) || 0;
        const productTypeId = Number(bp?.product_type_id || bp?.productTypeId || 0) || 0;
        if (!blueprintTypeId || !productTypeId) {
            return;
        }

        const userOwns = Boolean(
            bp?.user_owns
            ?? bp?.userOwns
            ?? bp?.is_owned
            ?? bp?.isOwned
        );
        const hasShared = Boolean(bp?.shared_copies_available ?? bp?.sharedCopiesAvailable);
        const available = userOwns || hasShared;
        const defaultSelected = false;
        const selected = selectedByBlueprintType.has(blueprintTypeId)
            ? selectedByBlueprintType.get(blueprintTypeId)
            : defaultSelected;

        const meInput = document.querySelector(`#configure-pane input[name="me_${blueprintTypeId}"]`);
        const teInput = document.querySelector(`#configure-pane input[name="te_${blueprintTypeId}"]`);
        const meValue = meInput ? (Number(meInput.value) || 0) : (Number(bp?.material_efficiency || 0) || 0);
        const teValue = teInput ? (Number(teInput.value) || 0) : (Number(bp?.time_efficiency || 0) || 0);
        const blueprintName = String(
            blueprintNames.get(blueprintTypeId)
            || bp?.type_name
            || bp?.typeName
            || `Blueprint ${blueprintTypeId}`
        ).trim();

        const entry = {
            blueprintTypeId,
            productTypeId,
            blueprintName,
            active: available && selected,
            selected: Boolean(selected),
            available: Boolean(available),
            owned: Boolean(userOwns),
            isCopy: Boolean(bp?.is_copy ?? bp?.isCopy) || (!userOwns && hasShared),
            me: meValue,
            te: teValue,
            defaultMe: 0,
            defaultTe: 0,
        };

        const existing = byProductType.get(productTypeId);
        if (!existing || (!existing.active && entry.active)) {
            byProductType.set(productTypeId, entry);
        }
    });

    // Fallback: include main blueprint if not present in configs
    const mainProductTypeId = Number(payload.product_type_id || payload.productTypeId || 0) || 0;
    const mainBlueprintTypeId = Number(payload.bp_type_id || payload.type_id || 0) || 0;
    if (mainProductTypeId && mainBlueprintTypeId && !byProductType.has(mainProductTypeId)) {
        const fallbackName = blueprintNames.get(mainBlueprintTypeId) || payload.name || `Blueprint ${mainBlueprintTypeId}`;
        byProductType.set(mainProductTypeId, {
            blueprintTypeId: mainBlueprintTypeId,
            productTypeId: mainProductTypeId,
            blueprintName: String(fallbackName).trim(),
            active: true,
            selected: true,
            available: true,
            owned: true,
            isCopy: Boolean(payload?.main_bp_info?.is_copy),
            me: Number(payload.me || 0) || 0,
            te: Number(payload.te || 0) || 0,
            defaultMe: 0,
            defaultTe: 0,
        });
    }

    return byProductType;
}

function getBlueprintUsageContextForProductType(typeId, contextsByProductType = null) {
    const productTypeId = Number(typeId) || 0;
    if (!productTypeId) {
        return null;
    }
    const contexts = contextsByProductType instanceof Map
        ? contextsByProductType
        : buildBlueprintUsageContextByProductType();
    return contexts.get(productTypeId) || null;
}

function isBlueprintActiveForProductType(typeId, contextsByProductType = null) {
    const context = getBlueprintUsageContextForProductType(typeId, contextsByProductType);
    if (!context) {
        return true;
    }
    return Boolean(context.active);
}

function formatBlueprintContextForPlanItem(typeId, contextsByProductType = null) {
    const context = getBlueprintUsageContextForProductType(typeId, contextsByProductType);
    if (!context) {
        return __('BPC: none');
    }
    if (!context.available) {
        return __('BPC: unavailable (BP ME 0 / Default ME 0)');
    }
    if (!context.active) {
        return __('BPC: not selected (BP ME 0 / Default ME 0)');
    }

    const sourceLabel = context.owned
        ? (context.isCopy ? __('owned copy') : __('owned original'))
        : __('shared copy');
    const bpMe = Number(context.me) || 0;
    const defaultMe = Number(context.defaultMe) || 0;
    const meDelta = bpMe - defaultMe;
    const deltaPrefix = meDelta >= 0 ? '+' : '';
    return `${__('BPC')}: ${context.blueprintName} (${sourceLabel}) | ${__('BP ME')}: ${bpMe} | ${__('Default ME')}: ${defaultMe} | ${__('Delta')}: ${deltaPrefix}${meDelta}`;
}

function formatBlueprintContextForTreeItem(typeId, contextsByProductType = null) {
    const numericTypeId = Number(typeId) || 0;
    if (!numericTypeId) {
        return '';
    }
    const context = getBlueprintUsageContextForProductType(numericTypeId, contextsByProductType);
    if (!context || !context.active) {
        return '';
    }
    const mode = getTreeSwitchModeForType(numericTypeId);
    if (mode !== 'prod') {
        return '';
    }
    const kind = context.isCopy ? __('BPC') : __('BPO');
    return `(${kind} ${__('ME')}: ${Number(context.me) || 0} ${__('TE')}: ${Number(context.te) || 0})`;
}

function refreshTreeBlueprintContextLabels() {
    const treeTab = document.getElementById('tab-tree');
    if (!treeTab) {
        return;
    }
    const contextsByProductType = buildBlueprintUsageContextByProductType();
    treeTab.querySelectorAll('summary[data-type-id]').forEach((summaryEl) => {
        const typeId = Number(summaryEl.getAttribute('data-type-id') || 0) || 0;
        const contextEl = summaryEl.querySelector('.tree-bp-context');
        if (!contextEl || !typeId) {
            return;
        }
        const text = formatBlueprintContextForTreeItem(typeId, contextsByProductType);
        contextEl.textContent = text;
        contextEl.classList.toggle('d-none', !text);
    });
}

function getAppliedMeForTreeSummary(summaryEl, contextsByProductType = null) {
    if (!summaryEl) {
        return 0;
    }

    const currentDetails = summaryEl.closest('details');
    const parentDetails = currentDetails && currentDetails.parentElement
        ? currentDetails.parentElement.closest('details')
        : null;

    // Top-level tree rows are governed by the main blueprint ME.
    if (!parentDetails) {
        return Math.max(0, Number(window.BLUEPRINT_DATA?.me || 0) || 0);
    }

    const parentSummary = parentDetails.querySelector('summary[data-type-id]');
    if (!parentSummary) {
        return 0;
    }
    const parentTypeId = Number(parentSummary.getAttribute('data-type-id') || 0) || 0;
    if (!parentTypeId) {
        return 0;
    }
    const parentMode = getTreeSwitchModeForType(parentTypeId);
    if (parentMode !== 'prod') {
        return 0;
    }
    const parentContext = getBlueprintUsageContextForProductType(parentTypeId, contextsByProductType);
    if (!parentContext || !parentContext.active) {
        return 0;
    }
    return Math.max(0, Number(parentContext.me) || 0);
}

function refreshTreeQuantityLabels() {
    const treeTab = document.getElementById('tab-tree');
    if (!treeTab) {
        return;
    }

    treeTab.querySelectorAll('summary[data-type-id]').forEach((summaryEl) => {
        const qtyWrap = summaryEl.querySelector('.tree-qty-wrap');
        const currentQtyEl = qtyWrap ? qtyWrap.querySelector('.tree-qty-current') : null;
        if (!qtyWrap || !currentQtyEl) {
            return;
        }

        const currentQtyRaw = Number(summaryEl.getAttribute('data-qty') || 0);
        const defaultQtyRaw = Number(summaryEl.getAttribute('data-qty-default') || currentQtyRaw || 0);
        const currentQty = Number.isFinite(currentQtyRaw) ? Math.max(0, Math.ceil(currentQtyRaw)) : 0;
        const defaultQty = Number.isFinite(defaultQtyRaw) ? Math.max(0, Math.ceil(defaultQtyRaw)) : 0;

        currentQtyEl.textContent = `x${formatInteger(currentQty)}`;

        let defaultWrap = qtyWrap.querySelector('.tree-qty-default-wrap');
        let defaultQtyEl = qtyWrap.querySelector('.tree-qty-default');

        if (currentQty !== defaultQty) {
            if (!defaultWrap) {
                defaultWrap = document.createElement('span');
                defaultWrap.className = 'ms-1 tree-qty-default-wrap';
                defaultWrap.innerHTML = `(<span class="text-decoration-line-through tree-qty-default"></span>)`;
                qtyWrap.appendChild(defaultWrap);
                defaultQtyEl = defaultWrap.querySelector('.tree-qty-default');
            }
            if (defaultQtyEl) {
                defaultQtyEl.textContent = `x${formatInteger(defaultQty)}`;
            }
            defaultWrap.classList.remove('d-none');
        } else if (defaultWrap) {
            defaultWrap.classList.add('d-none');
        }
    });
}

function getBlueprintConfigsForFinancialPlanner() {
    const payload = window.BLUEPRINT_DATA || {};
    if (Array.isArray(payload.blueprint_configs)) {
        return payload.blueprint_configs;
    }
    if (Array.isArray(payload.blueprintConfigs)) {
        return payload.blueprintConfigs;
    }
    return [];
}

function getCraftCyclesSummaryForFinancialPlanner() {
    const payload = window.BLUEPRINT_DATA || {};
    if (payload.craft_cycles_summary && typeof payload.craft_cycles_summary === 'object') {
        return payload.craft_cycles_summary;
    }
    if (payload.craftCyclesSummary && typeof payload.craftCyclesSummary === 'object') {
        return payload.craftCyclesSummary;
    }
    return {};
}

function getMainBlueprintInfoForFinancialPlanner() {
    const payload = window.BLUEPRINT_DATA || {};
    const mainBp = payload.main_bp_info || payload.mainBpInfo;
    if (mainBp && typeof mainBp === 'object') {
        return mainBp;
    }
    return {};
}

function isProductPlannedForProduction(productTypeId) {
    const numericProductTypeId = Number(productTypeId) || 0;
    if (!numericProductTypeId) {
        return false;
    }
    const mode = getTreeSwitchModeForType(numericProductTypeId);
    return mode === 'prod';
}

function syncConfigureCardFromSelectedContracts(blueprintTypeId) {
    const numericBlueprintTypeId = Number(blueprintTypeId) || 0;
    if (!numericBlueprintTypeId) {
        return;
    }

    const useInput = document.querySelector(`#configure-pane input.bp-use-input[data-blueprint-type-id="${numericBlueprintTypeId}"]`);
    const meInput = document.querySelector(`#configure-pane input[name="me_${numericBlueprintTypeId}"]`);
    const teInput = document.querySelector(`#configure-pane input[name="te_${numericBlueprintTypeId}"]`);
    if (!useInput) {
        return;
    }

    const selectedOffers = getSelectedContractOffersForBlueprint(numericBlueprintTypeId);
    if (selectedOffers.length === 0) {
        if (useInput.dataset.contractDriven === 'true') {
            useInput.checked = false;
            useInput.dataset.contractDriven = 'false';
            useInput.dispatchEvent(new Event('change', { bubbles: true }));
        }
        return;
    }

    const primaryOffer = selectedOffers[0];
    if (!useInput.disabled) {
        useInput.checked = true;
        useInput.dataset.contractDriven = 'true';
        useInput.dispatchEvent(new Event('change', { bubbles: true }));
    }
    if (meInput) {
        meInput.value = String(Math.max(0, Math.min(Number(primaryOffer.me) || 0, 10)));
        meInput.dispatchEvent(new Event('input', { bubbles: true }));
    }
    if (teInput) {
        teInput.value = String(Math.max(0, Math.min(Number(primaryOffer.te) || 0, 20)));
        teInput.dispatchEvent(new Event('input', { bubbles: true }));
    }
}

function syncConfigureVisibilityWithPlan() {
    const configurePane = document.getElementById('configure-pane');
    if (!configurePane) {
        return;
    }

    configurePane.querySelectorAll('.craft-bp-card[data-blueprint-type-id]').forEach((card) => {
        const productTypeId = Number(card.getAttribute('data-product-type-id')) || 0;
        if (!productTypeId) {
            return;
        }
        const keepVisible = isProductPlannedForProduction(productTypeId);
        card.classList.toggle('d-none', !keepVisible);
        const useInput = card.querySelector('input.bp-use-input[data-blueprint-type-id]');
        if (!keepVisible && useInput && useInput.checked) {
            useInput.checked = false;
            useInput.dataset.contractDriven = 'false';
            useInput.dispatchEvent(new Event('change', { bubbles: true }));
        }
    });

    configurePane.querySelectorAll('.craft-config-items-grid').forEach((grid) => {
        const visibleCards = grid.querySelectorAll('.craft-bp-card:not(.d-none)');
        const levelContainer = grid.closest('.craft-config-level');
        if (levelContainer) {
            levelContainer.classList.toggle('d-none', visibleCards.length === 0);
        }
    });
}

function collectBlueprintCopyDeficits() {
    const blueprintConfigs = getBlueprintConfigsForFinancialPlanner();
    if (!Array.isArray(blueprintConfigs) || blueprintConfigs.length === 0) {
        return [];
    }

    const cyclesSummary = getCraftCyclesSummaryForFinancialPlanner();
    const mainBpInfo = getMainBlueprintInfoForFinancialPlanner();
    const blueprintNames = getBlueprintNameMapFromConfigPane();
    const selectedByBlueprintType = getBlueprintUseSelectionFromDom();
    const runsInputEl = document.getElementById('runsInput');

    const mainBpTypeId = Number(
        mainBpInfo.type_id
        || mainBpInfo.typeId
        || (window.BLUEPRINT_DATA && (window.BLUEPRINT_DATA.bp_type_id || window.BLUEPRINT_DATA.type_id))
        || 0
    ) || 0;
    const mainNumRuns = runsInputEl
        ? (Number(runsInputEl.value) || 0)
        : (Number(window.BLUEPRINT_DATA?.num_runs) || 0);
    const mainAvailableRuns = (mainBpInfo && Boolean(mainBpInfo.is_copy) && mainBpInfo.runs_available != null)
        ? (Number(mainBpInfo.runs_available) || 0)
        : null;

    const deficits = [];
    blueprintConfigs.forEach((bp) => {
        const blueprintTypeId = Number(bp?.type_id || bp?.typeId) || 0;
        if (!blueprintTypeId) {
            return;
        }

        const productTypeId = Number(bp?.product_type_id || bp?.productTypeId || blueprintTypeId) || 0;
        if (!isProductPlannedForProduction(productTypeId)) {
            return;
        }

        const cyclesData = cyclesSummary[productTypeId]
            || cyclesSummary[String(productTypeId)]
            || cyclesSummary[blueprintTypeId]
            || cyclesSummary[String(blueprintTypeId)];

        let requiredRuns = cyclesData ? (Number(cyclesData.cycles) || 0) : null;
        if (requiredRuns === null && mainBpTypeId && blueprintTypeId === mainBpTypeId) {
            requiredRuns = mainNumRuns;
        }
        if (requiredRuns === null) {
            return;
        }
        requiredRuns = Math.max(0, Number(requiredRuns) || 0);
        if (requiredRuns <= 0) {
            return;
        }

        const userOwns = Boolean(
            bp?.user_owns
            ?? bp?.userOwns
            ?? bp?.is_owned
            ?? bp?.isOwned
        );
        const isCopy = Boolean(bp?.is_copy ?? bp?.isCopy);
        const runsAvailableRaw = bp?.runs_available ?? bp?.runsAvailable;
        const hasRunsAvailable = runsAvailableRaw !== null && runsAvailableRaw !== undefined && runsAvailableRaw !== '';
        const hasOriginal = userOwns && !isCopy;
        const selectedInConfigure = Boolean(selectedByBlueprintType.get(blueprintTypeId));

        let availableRuns = 0;
        if (selectedInConfigure) {
            if (hasOriginal) {
                availableRuns = Number.POSITIVE_INFINITY;
            } else if (userOwns && isCopy && hasRunsAvailable) {
                availableRuns = Number(runsAvailableRaw) || 0;
            }
        }
        if (mainBpTypeId && blueprintTypeId === mainBpTypeId && mainAvailableRuns !== null && selectedInConfigure) {
            availableRuns = mainAvailableRuns;
        }

        const selectedContractRuns = getSelectedContractRunsForBlueprint(blueprintTypeId);
        const effectiveAvailableRuns = Number.isFinite(availableRuns)
            ? Math.max(0, availableRuns + selectedContractRuns)
            : Number.POSITIVE_INFINITY;
        const deficitRuns = Number.isFinite(effectiveAvailableRuns)
            ? Math.max(0, Math.ceil(requiredRuns - effectiveAvailableRuns))
            : 0;

        const fallbackName = String(cyclesData?.type_name || cyclesData?.typeName || '').trim();
        const blueprintName = String(
            blueprintNames.get(blueprintTypeId)
            || bp?.type_name
            || bp?.typeName
            || fallbackName
            || `Blueprint ${blueprintTypeId}`
        ).trim();

        deficits.push({
            blueprintTypeId,
            productTypeId,
            blueprintName,
            requiredRuns,
            ownedRuns: Number.isFinite(availableRuns) ? Math.max(0, Number(availableRuns) || 0) : Number.POSITIVE_INFINITY,
            selectedRuns: selectedContractRuns,
            deficitRuns,
            selectedInConfigure,
            userOwns,
            isCopy,
            hasOriginal,
        });
    });

    deficits.sort((left, right) => String(left.blueprintName).localeCompare(String(right.blueprintName), undefined, { sensitivity: 'base' }));
    return deficits;
}

function collectMissingBlueprintCopyRows() {
    return collectBlueprintCopyDeficits()
        .filter((row) => Number(row.deficitRuns) > 0)
        .map((row) => ({
            typeId: row.blueprintTypeId,
            typeName: row.blueprintName,
            quantity: Math.max(1, Number(row.deficitRuns) || 0),
            marketGroup: __('Blueprint Copies'),
            rowKind: 'bpc',
            rowKey: `bpc-missing:${row.blueprintTypeId}`,
        }));
}

function collectSelectedBlueprintContractRows() {
    pruneSelectedContractsToCurrentProductionPlan();
    const rows = [];
    const blueprintNameMap = getBlueprintNameMapFromConfigPane();
    CRAFT_BPC_CONTRACT_STATE.selectedByBlueprintType.forEach((offerMap, blueprintTypeId) => {
        if (!(offerMap instanceof Map) || offerMap.size === 0) {
            return;
        }
        const blueprintName = String(
            blueprintNameMap.get(blueprintTypeId)
            || `Blueprint ${blueprintTypeId}`
        ).trim();

        offerMap.forEach((offer, contractId) => {
            const runs = Math.max(1, Number(offer.runs) || 1);
            const totalPrice = Math.max(0, Number(offer.price_total) || 0);
            const unitPrice = runs > 0 ? (totalPrice / runs) : totalPrice;
            rows.push({
                typeId: blueprintTypeId,
                typeName: `${blueprintName} (Contract #${contractId}, ME ${Number(offer.me) || 0}, TE ${Number(offer.te) || 0})`,
                quantity: runs,
                marketGroup: __('Blueprint Copies'),
                rowKind: 'bpc',
                rowKey: `bpc-contract:${blueprintTypeId}:${contractId}`,
                unitPrice,
            });
        });
    });
    rows.sort((left, right) => String(left.typeName).localeCompare(String(right.typeName), undefined, { sensitivity: 'base' }));
    return rows;
}

function getBlueprintConfigByTypeIdMap() {
    const map = new Map();
    getBlueprintConfigsForFinancialPlanner().forEach((bp) => {
        const blueprintTypeId = Number(bp?.type_id || bp?.typeId) || 0;
        if (!blueprintTypeId) {
            return;
        }
        map.set(blueprintTypeId, bp);
    });
    return map;
}

function pruneSelectedContractsToCurrentProductionPlan() {
    const configByTypeId = getBlueprintConfigByTypeIdMap();
    let changed = false;
    Array.from(CRAFT_BPC_CONTRACT_STATE.selectedByBlueprintType.keys()).forEach((blueprintTypeId) => {
        const bp = configByTypeId.get(blueprintTypeId);
        const productTypeId = Number(bp?.product_type_id || bp?.productTypeId || 0) || 0;
        if (!bp || !productTypeId || !isProductPlannedForProduction(productTypeId)) {
            CRAFT_BPC_CONTRACT_STATE.selectedByBlueprintType.delete(blueprintTypeId);
            syncConfigureCardFromSelectedContracts(blueprintTypeId);
            changed = true;
        }
    });
    if (changed) {
        saveSelectedBpcContractsToStorage();
    }
}

function getCraftBpcContractsUrl() {
    return String(window.BLUEPRINT_DATA?.urls?.craft_bpc_contracts || '').trim();
}

async function fetchBuyBpcOffersForBlueprints(blueprintTypeIds, { force = false } = {}) {
    const endpoint = getCraftBpcContractsUrl();
    if (!endpoint) {
        return;
    }
    const uniqueIds = Array.from(new Set((Array.isArray(blueprintTypeIds) ? blueprintTypeIds : [])
        .map((id) => Number(id) || 0)
        .filter((id) => id > 0)));
    if (uniqueIds.length === 0) {
        return;
    }

    const idsToFetch = force
        ? uniqueIds
        : uniqueIds.filter((id) => !CRAFT_BPC_CONTRACT_STATE.offersByBlueprintType.has(id));
    if (idsToFetch.length === 0) {
        return;
    }

    const requestUrl = new URL(endpoint, window.location.origin);
    requestUrl.searchParams.set('blueprint_type_ids', idsToFetch.join(','));

    const response = await fetch(requestUrl.toString(), {
        method: 'GET',
        credentials: 'same-origin',
    });
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    const contractMap = payload && payload.contracts_by_blueprint && typeof payload.contracts_by_blueprint === 'object'
        ? payload.contracts_by_blueprint
        : {};

    idsToFetch.forEach((blueprintTypeId) => {
        const rawOffers = contractMap[String(blueprintTypeId)];
        const normalizedOffers = Array.isArray(rawOffers)
            ? rawOffers
                .map((offer) => ({
                    blueprint_type_id: blueprintTypeId,
                    contract_id: Number(offer?.contract_id) || 0,
                    title: String(offer?.title || '').trim(),
                    price_total: Number(offer?.price_total) || 0,
                    price_per_run: Number(offer?.price_per_run) || 0,
                    runs: Math.max(1, Number(offer?.runs) || 1),
                    copies: Math.max(1, Number(offer?.copies) || 1),
                    me: Number(offer?.me) || 0,
                    te: Number(offer?.te) || 0,
                    issued_at: String(offer?.issued_at || ''),
                    expires_at: String(offer?.expires_at || ''),
                }))
                .filter((offer) => offer.contract_id > 0)
            : [];
        CRAFT_BPC_CONTRACT_STATE.offersByBlueprintType.set(blueprintTypeId, normalizedOffers);
    });
}

function getBuyBpcsSortKey() {
    const selectEl = document.getElementById('buyBpcsSortSelect');
    return String(selectEl ? selectEl.value : 'effective');
}

function sortBuyBpcOffers(offers, deficitRuns, sortKey) {
    const rows = Array.isArray(offers) ? [...offers] : [];
    const normalizedSortKey = String(sortKey || 'effective');
    rows.sort((left, right) => {
        const leftRuns = Math.max(1, Number(left.runs) || 1);
        const rightRuns = Math.max(1, Number(right.runs) || 1);
        const leftPrice = Math.max(0, Number(left.price_total) || 0);
        const rightPrice = Math.max(0, Number(right.price_total) || 0);
        const leftEffectiveRuns = Math.max(1, Math.min(leftRuns, Math.max(1, Number(deficitRuns) || 1)));
        const rightEffectiveRuns = Math.max(1, Math.min(rightRuns, Math.max(1, Number(deficitRuns) || 1)));
        const leftEffective = leftPrice / leftEffectiveRuns;
        const rightEffective = rightPrice / rightEffectiveRuns;

        if (normalizedSortKey === 'total_price' && leftPrice !== rightPrice) {
            return leftPrice - rightPrice;
        }
        if (normalizedSortKey === 'runs_desc' && leftRuns !== rightRuns) {
            return rightRuns - leftRuns;
        }
        if (normalizedSortKey === 'me_desc' && Number(left.me) !== Number(right.me)) {
            return Number(right.me) - Number(left.me);
        }
        if (normalizedSortKey === 'te_desc' && Number(left.te) !== Number(right.te)) {
            return Number(right.te) - Number(left.te);
        }
        if (normalizedSortKey === 'newest') {
            const leftIssued = Date.parse(String(left.issued_at || '')) || 0;
            const rightIssued = Date.parse(String(right.issued_at || '')) || 0;
            if (leftIssued !== rightIssued) {
                return rightIssued - leftIssued;
            }
        }

        if (leftEffective !== rightEffective) {
            return leftEffective - rightEffective;
        }
        if (leftPrice !== rightPrice) {
            return leftPrice - rightPrice;
        }
        return leftRuns - rightRuns;
    });
    return rows;
}

function setBuyBpcsStatus(message, variant = 'info') {
    const statusEl = document.getElementById('buyBpcsStatus');
    if (!statusEl) {
        return;
    }
    if (!message) {
        statusEl.className = 'alert alert-info mb-3 d-none';
        statusEl.textContent = '';
        return;
    }
    statusEl.className = `alert alert-${variant} mb-3`;
    statusEl.textContent = String(message);
}

function renderBuyBpcsTab() {
    const listEl = document.getElementById('buyBpcsList');
    const emptyEl = document.getElementById('buyBpcsEmptyState');
    if (!listEl || !emptyEl) {
        return;
    }

    pruneSelectedContractsToCurrentProductionPlan();

    const deficits = collectBlueprintCopyDeficits().filter((row) => Number(row.deficitRuns) > 0);
    listEl.innerHTML = '';

    if (deficits.length === 0) {
        emptyEl.classList.remove('d-none');
        return;
    }

    emptyEl.classList.add('d-none');
    const paneEl = document.getElementById('buy-bpcs-pane');
    const tabIsActive = Boolean(paneEl && paneEl.classList.contains('active'));
    if (tabIsActive && !CRAFT_BPC_CONTRACT_STATE.loading) {
        const missingOfferIds = deficits
            .map((row) => Number(row.blueprintTypeId) || 0)
            .filter((blueprintTypeId) => blueprintTypeId > 0 && !CRAFT_BPC_CONTRACT_STATE.offersByBlueprintType.has(blueprintTypeId));
        if (missingOfferIds.length > 0) {
            refreshBuyBpcsOffers({ force: false });
        }
    }
    const sortKey = getBuyBpcsSortKey();

    deficits.forEach((row) => {
        const blueprintTypeId = Number(row.blueprintTypeId) || 0;
        const offers = CRAFT_BPC_CONTRACT_STATE.offersByBlueprintType.get(blueprintTypeId) || [];
        const selectedOffers = getSelectedContractOffersForBlueprint(blueprintTypeId);
        const selectedIds = new Set(selectedOffers.map((offer) => Number(offer.contract_id) || 0));
        const sortedOffers = sortBuyBpcOffers(offers, row.deficitRuns, sortKey);

        const card = document.createElement('section');
        card.className = 'card border-0 bg-body-tertiary';
        card.innerHTML = `
            <div class="card-body">
                <div class="d-flex flex-wrap justify-content-between align-items-start gap-2 mb-2">
                    <div>
                        <h6 class="mb-1">${escapeHtml(row.blueprintName)}</h6>
                        <div class="small text-muted">
                            ${escapeHtml(__('Required runs'))}: <strong>${formatInteger(row.requiredRuns)}</strong>
                            - ${escapeHtml(__('Owned/selected'))}: <strong>${formatInteger(Number(row.ownedRuns) || 0)} / ${formatInteger(row.selectedRuns)}</strong>
                            - ${escapeHtml(__('Missing'))}: <strong class="text-danger">${formatInteger(row.deficitRuns)}</strong>
                        </div>
                    </div>
                </div>
                <div class="table-responsive">
                    <table class="table table-sm align-middle mb-0">
                        <thead>
                            <tr>
                                <th>${escapeHtml(__('Contract'))}</th>
                                <th class="text-end">${escapeHtml(__('Price'))}</th>
                                <th class="text-end">${escapeHtml(__('Runs'))}</th>
                                <th class="text-end">ME</th>
                                <th class="text-end">TE</th>
                                <th class="text-end">${escapeHtml(__('ISK/run'))}</th>
                                <th class="text-end">${escapeHtml(__('Action'))}</th>
                            </tr>
                        </thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        `;

        const tbody = card.querySelector('tbody');
        if (!tbody) {
            listEl.appendChild(card);
            return;
        }

        if (sortedOffers.length === 0) {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td colspan="7" class="text-muted small">
                    ${escapeHtml(__('No matching public Jita contracts found right now.'))}
                </td>
            `;
            tbody.appendChild(tr);
            listEl.appendChild(card);
            return;
        }

        sortedOffers.slice(0, 25).forEach((offer) => {
            const contractId = Number(offer.contract_id) || 0;
            const isSelected = selectedIds.has(contractId);
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td class="small">
                    <strong>#${contractId}</strong>
                    ${offer.title ? `<span class="text-muted"> - ${escapeHtml(offer.title)}</span>` : ''}
                </td>
                <td class="text-end">${formatPrice(Number(offer.price_total) || 0)}</td>
                <td class="text-end">${formatInteger(Number(offer.runs) || 0)}</td>
                <td class="text-end">${formatInteger(Number(offer.me) || 0)}</td>
                <td class="text-end">${formatInteger(Number(offer.te) || 0)}</td>
                <td class="text-end">${formatPrice(Number(offer.price_per_run) || 0)}</td>
                <td class="text-end">
                    <button type="button" class="btn btn-sm ${isSelected ? 'btn-success' : 'btn-outline-primary'}" data-bpc-offer-toggle="1">
                        ${isSelected ? escapeHtml(__('Using')) : escapeHtml(__('Use this'))}
                    </button>
                </td>
            `;

            const button = tr.querySelector('button[data-bpc-offer-toggle="1"]');
            if (button) {
                button.addEventListener('click', () => {
                    if (isSelected) {
                        removeSelectedContractOffer(blueprintTypeId, contractId);
                    } else {
                        upsertSelectedContractOffer(blueprintTypeId, offer);
                    }
                    syncConfigureCardFromSelectedContracts(blueprintTypeId);
                    saveSelectedBpcContractsToStorage();
                    refreshTabsAfterStateChange({ forceNeeded: true });
                });
            }

            tbody.appendChild(tr);
        });
        listEl.appendChild(card);
    });
}

async function refreshBuyBpcsOffers({ force = false } = {}) {
    const deficits = collectBlueprintCopyDeficits().filter((row) => Number(row.deficitRuns) > 0);
    const blueprintTypeIds = deficits.map((row) => Number(row.blueprintTypeId) || 0).filter((id) => id > 0);
    if (blueprintTypeIds.length === 0) {
        setBuyBpcsStatus('', 'info');
        renderBuyBpcsTab();
        return;
    }

    CRAFT_BPC_CONTRACT_STATE.loading = true;
    setBuyBpcsStatus(__('Loading public Jita contracts...'), 'info');
    try {
        await fetchBuyBpcOffersForBlueprints(blueprintTypeIds, { force });
        setBuyBpcsStatus('', 'info');
    } catch (error) {
        console.error('[CraftBP] Failed loading BPC contract offers', error);
        setBuyBpcsStatus(__('Unable to load public Jita contracts right now.'), 'warning');
    } finally {
        CRAFT_BPC_CONTRACT_STATE.loading = false;
        renderBuyBpcsTab();
    }
}

function initializeBuyBpcsTab() {
    if (CRAFT_BPC_CONTRACT_STATE.loaded) {
        return;
    }
    CRAFT_BPC_CONTRACT_STATE.loaded = true;

    const sortSelect = document.getElementById('buyBpcsSortSelect');
    if (sortSelect) {
        sortSelect.addEventListener('change', () => {
            renderBuyBpcsTab();
        });
    }

    const refreshBtn = document.getElementById('buyBpcsRefreshBtn');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', () => {
            refreshBuyBpcsOffers({ force: true });
        });
    }

    const tabButton = document.getElementById('buy-bpcs-tab-btn');
    if (tabButton) {
        tabButton.addEventListener('shown.bs.tab', () => {
            refreshBuyBpcsOffers({ force: false });
        });
    }

    CRAFT_BPC_CONTRACT_STATE.selectedByBlueprintType.forEach((_offerMap, blueprintTypeId) => {
        syncConfigureCardFromSelectedContracts(blueprintTypeId);
    });

    renderBuyBpcsTab();
}

function updateFinancialTabFromState() {
    const tableBody = document.getElementById('financialItemsBody');
    if (!tableBody || !window.SimulationAPI || typeof window.SimulationAPI.getFinancialItems !== 'function') {
        return;
    }

    const finalRow = document.getElementById('finalProductRow');
    const productTypeId = getProductTypeIdValue();
    const pricesMap = getSimulationPricesMap();

    const aggregated = new Map();
    const items = window.SimulationAPI.getFinancialItems() || [];

    items.forEach(item => {
        const typeId = Number(item.typeId ?? item.type_id);
        if (!typeId || (productTypeId && typeId === productTypeId)) {
            return;
        }
        const quantity = Math.ceil(Number(item.quantity ?? item.qty ?? 0));
        if (quantity <= 0) {
            return;
        }
        const existing = aggregated.get(typeId) || {
            typeId,
            typeName: item.typeName || item.type_name || '',
            quantity: 0,
            marketGroup: item.marketGroup || item.market_group || '',
            rowKind: 'material',
            rowKey: `material:${typeId}`,
        };
        existing.quantity += quantity;
        if (!existing.marketGroup && (item.marketGroup || item.market_group)) {
            existing.marketGroup = item.marketGroup || item.market_group || '';
        }
        aggregated.set(typeId, existing);
    });

    const ordering = getDashboardMaterialsOrdering();
    const sortedMaterials = Array.from(aggregated.values()).sort((a, b) => {
        const typeA = Number(a.typeId) || 0;
        const typeB = Number(b.typeId) || 0;

        const dashboardA = ordering.itemOrder.get(typeA);
        const dashboardB = ordering.itemOrder.get(typeB);
        const groupA = (a.marketGroup || ordering.fallbackGroupName);
        const groupB = (b.marketGroup || ordering.fallbackGroupName);

        const groupIdxA = dashboardA ? dashboardA.groupIdx : (ordering.groupOrder.has(groupA) ? ordering.groupOrder.get(groupA) : Number.POSITIVE_INFINITY);
        const groupIdxB = dashboardB ? dashboardB.groupIdx : (ordering.groupOrder.has(groupB) ? ordering.groupOrder.get(groupB) : Number.POSITIVE_INFINITY);
        if (groupIdxA !== groupIdxB) {
            return groupIdxA - groupIdxB;
        }

        // If both are in the dashboard materials list, keep the exact dashboard item order.
        const itemIdxA = dashboardA ? dashboardA.itemIdx : Number.POSITIVE_INFINITY;
        const itemIdxB = dashboardB ? dashboardB.itemIdx : Number.POSITIVE_INFINITY;
        if (itemIdxA !== itemIdxB) {
            return itemIdxA - itemIdxB;
        }

        // Fallbacks (for craftables not present on the dashboard materials list)
        const groupCmp = String(groupA).localeCompare(String(groupB), undefined, { sensitivity: 'base' });
        if (groupCmp !== 0) {
            return groupCmp;
        }
        return String(a.typeName).localeCompare(String(b.typeName), undefined, { sensitivity: 'base' });
    });
    const selectedBpcRows = collectSelectedBlueprintContractRows();
    const missingBpcRows = collectMissingBlueprintCopyRows();
    const sortedItems = [...selectedBpcRows, ...missingBpcRows, ...sortedMaterials];

    const existingRows = new Map();
    tableBody.querySelectorAll('tr[data-type-id]').forEach(row => {
        if (finalRow && row === finalRow) {
            return;
        }
        const typeId = Number(row.getAttribute('data-type-id')) || 0;
        if (!typeId) {
            return;
        }
        const explicitKey = String(row.getAttribute('data-row-key') || '').trim();
        const rowKind = String(row.getAttribute('data-row-kind') || 'material').toLowerCase() === 'bpc' ? 'bpc' : 'material';
        const rowKey = explicitKey || `${rowKind}:${typeId}`;
        existingRows.set(rowKey, row);
    });

    const newRows = [];

    sortedItems.forEach(item => {
        const rowKey = getFinancialRowKey(item);
        if (!rowKey) {
            return;
        }
        let row = existingRows.get(rowKey);
        if (row) {
            updateFinancialRow(row, item);
            tableBody.insertBefore(row, finalRow || null);
            existingRows.delete(rowKey);
        } else {
            const buildResult = buildFinancialRow(item, pricesMap);
            row = buildResult.row;
            tableBody.insertBefore(row, finalRow || null);
            newRows.push(buildResult);
        }
    });

    existingRows.forEach(row => row.remove());

    if (finalRow && finalRow.parentElement !== tableBody) {
        tableBody.appendChild(finalRow);
    }

    if (newRows.length > 0) {
        const typeIds = Array.from(new Set(
            newRows
                .map(entry => Number(entry.typeId) || 0)
                .filter((typeId) => typeId > 0)
                .map((typeId) => String(typeId))
        ));
        fetchAllPrices(typeIds).then(prices => {
            newRows.forEach(({ typeId, fuzzInput, realInput }) => {
                const priceValue = parseFloat(prices[typeId] ?? prices[String(typeId)]) || 0;
                fuzzInput.value = priceValue.toFixed(2);
                if (priceValue <= 0) {
                    fuzzInput.classList.add('bg-warning', 'border-warning');
                    fuzzInput.setAttribute('title', __('Price not available (Fuzzwork)'));
                } else {
                    fuzzInput.classList.remove('bg-warning', 'border-warning');
                    fuzzInput.removeAttribute('title');
                }
                if (window.SimulationAPI && typeof window.SimulationAPI.setPrice === 'function') {
                    window.SimulationAPI.setPrice(typeId, 'fuzzwork', priceValue);
                }
                // Real Price stays at 0 by default; do not copy Fuzzwork
            });
            if (typeof recalcFinancials === 'function') {
                recalcFinancials();
            }
        });
    } else if (typeof recalcFinancials === 'function') {
        recalcFinancials();
    }
}

function getCraftFinalProductLabel() {
    const payload = window.BLUEPRINT_DATA || {};
    const payloadLabel = String(
        payload.bp_name
        || payload.product_name
        || payload.productName
        || payload.type_name
        || payload.typeName
        || ''
    ).trim();
    if (payloadLabel) {
        return payloadLabel;
    }

    const buildTabLabel = document.querySelector('#build-pane .table-primary .small.fw-bold');
    if (buildTabLabel && buildTabLabel.textContent) {
        const text = String(buildTabLabel.textContent).trim();
        if (text) {
            return text;
        }
    }

    return __('Final product');
}

function getTreeSwitchModeForType(typeId) {
    const numericTypeId = Number(typeId) || 0;
    if (!numericTypeId) {
        return 'prod';
    }

    if (window.SimulationAPI && typeof window.SimulationAPI.getSwitchState === 'function') {
        const stateFromApi = window.SimulationAPI.getSwitchState(numericTypeId);
        if (stateFromApi === 'buy' || stateFromApi === 'prod' || stateFromApi === 'useless') {
            return stateFromApi;
        }
    }

    const switchEl = document.querySelector(`#tab-tree input.mat-switch[data-type-id="${numericTypeId}"]`);
    if (switchEl) {
        if (switchEl.dataset.fixedMode === 'useless' || switchEl.dataset.userState === 'useless') {
            return 'useless';
        }
        return switchEl.checked ? 'prod' : 'buy';
    }
    return 'prod';
}

function buildMaterialUsageTargetsByType() {
    const usageByType = new Map();
    const tree = window.BLUEPRINT_DATA && Array.isArray(window.BLUEPRINT_DATA.materials_tree)
        ? window.BLUEPRINT_DATA.materials_tree
        : [];
    if (tree.length === 0) {
        return usageByType;
    }

    const finalProductLabel = getCraftFinalProductLabel();

    const addUsage = (typeId, targetName) => {
        const numericTypeId = Number(typeId) || 0;
        if (!numericTypeId) {
            return;
        }
        const normalizedTarget = String(targetName || '').trim() || finalProductLabel;
        if (!usageByType.has(numericTypeId)) {
            usageByType.set(numericTypeId, new Set());
        }
        usageByType.get(numericTypeId).add(normalizedTarget);
    };

    const walk = (nodes, currentTargetName) => {
        (Array.isArray(nodes) ? nodes : []).forEach((node) => {
            const typeId = Number(node?.type_id || node?.typeId) || 0;
            if (!typeId) {
                return;
            }

            const qty = Math.max(0, Math.ceil(Number(node?.quantity ?? node?.qty ?? 0))) || 0;
            if (!qty) {
                return;
            }

            const typeName = String(node?.type_name || node?.typeName || '').trim();
            const children = Array.isArray(node?.sub_materials)
                ? node.sub_materials
                : (Array.isArray(node?.subMaterials) ? node.subMaterials : []);
            const craftable = children.length > 0;
            const currentTarget = currentTargetName || finalProductLabel;

            if (!craftable) {
                addUsage(typeId, currentTarget);
                return;
            }

            const mode = getTreeSwitchModeForType(typeId);
            if (mode === 'useless') {
                return;
            }
            if (mode === 'buy') {
                addUsage(typeId, currentTarget);
                return;
            }

            const nextTarget = typeName || currentTarget;
            walk(children, nextTarget);
        });
    };

    walk(tree, finalProductLabel);
    return usageByType;
}

function formatUsageSummaryText(targetNamesSet) {
    const targetNames = Array.from(targetNamesSet || [])
        .map((name) => String(name || '').trim())
        .filter(Boolean)
        .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }));

    if (targetNames.length === 0) {
        return __('Used in current production chain');
    }

    const visibleTargets = targetNames.slice(0, 3);
    const baseText = `${__('Used to make')}: ${visibleTargets.join(', ')}`;
    if (targetNames.length <= visibleTargets.length) {
        return baseText;
    }

    const remainder = targetNames.length - visibleTargets.length;
    return `${baseText} (+${remainder} ${__('more')})`;
}

function updateMaterialsTabFromState() {
    const container = document.getElementById('materialsGroupsContainer');
    if (!container || !window.SimulationAPI || typeof window.SimulationAPI.getFinancialItems !== 'function') {
        return;
    }

    const emptyState = document.getElementById('materialsEmptyState');
    const productTypeId = getProductTypeIdValue();
    const fallbackGroupName = __('Other');
    const aggregated = new Map();
    const usageByType = buildMaterialUsageTargetsByType();
    const treeScopedItems = computeNeededItemsFromTreeWithOwned(window.SimulationAPI, new Map());
    const items = Array.isArray(CRAFT_COMPUTED_NEEDED_ROWS)
        ? CRAFT_COMPUTED_NEEDED_ROWS
        : (Array.isArray(treeScopedItems) && treeScopedItems.length > 0
            ? treeScopedItems
            : (window.SimulationAPI.getFinancialItems() || []));

    items.forEach(item => {
        const typeId = Number(item.typeId ?? item.type_id);
        if (!typeId || (productTypeId && typeId === productTypeId)) {
            return;
        }
        const quantity = Math.ceil(Number(item.quantity ?? item.qty ?? 0));
        if (quantity <= 0) {
            return;
        }
        const existing = aggregated.get(typeId) || {
            typeId,
            typeName: item.typeName || item.type_name || item.name || '',
            quantity: 0,
            marketGroup: item.marketGroup || item.market_group || ''
        };
        existing.quantity += quantity;
        if (!existing.marketGroup && (item.marketGroup || item.market_group)) {
            existing.marketGroup = item.marketGroup || item.market_group || '';
        }
        aggregated.set(typeId, existing);
    });

    const groups = new Map();
    aggregated.forEach(entry => {
        const groupName = entry.marketGroup ? entry.marketGroup : fallbackGroupName;
        if (!groups.has(groupName)) {
            groups.set(groupName, []);
        }
        groups.get(groupName).push(entry);
    });

    if (groups.size === 0) {
        container.innerHTML = '';
        if (emptyState) {
            emptyState.style.display = '';
        }
        return;
    }

    const sortedGroups = Array.from(groups.entries()).sort((a, b) => a[0].localeCompare(b[0], undefined, { sensitivity: 'base' }));
    container.innerHTML = '';

    sortedGroups.forEach(([groupName, groupItems]) => {
        groupItems.sort((a, b) => a.typeName.localeCompare(b.typeName, undefined, { sensitivity: 'base' }));
        const usageTargets = new Set();
        groupItems.forEach((item) => {
            const targets = usageByType.get(Number(item.typeId) || 0);
            if (!targets) {
                return;
            }
            targets.forEach((name) => usageTargets.add(name));
        });
        const usageSummary = formatUsageSummaryText(usageTargets);

        const rowsHtml = groupItems.map(item => `
            <tr data-type-id="${item.typeId}">
                <td class="fw-semibold">
                    <div class="d-flex align-items-center gap-3">
                        <img src="https://images.evetech.net/types/${item.typeId}/icon?size=32" alt="${escapeHtml(item.typeName)}" class="rounded" style="width:30px;height:30px;background:#f3f4f6;" onerror="this.style.display='none';">
                        <span class="badge bg-info-subtle text-info-emphasis px-2 py-1">${escapeHtml(item.typeName)}</span>
                    </div>
                </td>
                <td class="text-end">
                    <span class="badge bg-primary text-white" data-qty="${item.quantity}">${formatInteger(item.quantity)}</span>
                </td>
                <td class="small text-muted">${escapeHtml(formatUsageSummaryText(usageByType.get(Number(item.typeId) || 0) || new Set()))}</td>
            </tr>
        `).join('');

        const card = document.createElement('div');
        card.className = 'card shadow-sm mb-4';
        card.innerHTML = `
            <div class="card-header d-flex align-items-center justify-content-between bg-body-secondary">
                <div class="me-2">
                    <div class="fw-semibold">
                        <i class="fas fa-layer-group text-primary me-2"></i>${escapeHtml(groupName)}
                    </div>
                    <div class="small text-muted mt-1">
                        <i class="fas fa-gears me-1"></i>${escapeHtml(usageSummary)}
                    </div>
                </div>
                <span class="badge bg-primary-subtle text-primary fw-semibold">${groupItems.length}</span>
            </div>
            <div class="card-body p-0">
                <div class="table-responsive">
                    <table class="table table-hover table-sm align-middle mb-0">
                        <thead class="table-light">
                            <tr>
                                <th>${__('Material')}</th>
                                <th class="text-end">${__('Quantity')}</th>
                                <th>${__('Needed for')}</th>
                            </tr>
                        </thead>
                        <tbody>${rowsHtml}</tbody>
                    </table>
                </div>
            </div>
        `;
        container.appendChild(card);
    });

    if (emptyState) {
        emptyState.style.display = 'none';
    }
}

function updateNeededTabFromState(force = false) {
    const neededTab = document.getElementById('tab-needed');
    if (!neededTab) {
        return;
    }
    if (!force && !neededTab.classList.contains('active')) {
        return;
    }
    if (typeof computeNeededPurchases === 'function') {
        computeNeededPurchases();
    }
}

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
    const payload = window.BLUEPRINT_DATA || {};
    const nameToTypeIds = new Map();
    const byType = new Map();
    const matchedNames = new Set();
    const unresolvedNames = [];

    const addNameType = (typeId, typeName) => {
        const numericTypeId = Number(typeId) || 0;
        const normalizedName = normalizeOwnedMaterialName(typeName);
        if (!numericTypeId || !normalizedName) {
            return;
        }
        if (!nameToTypeIds.has(normalizedName)) {
            nameToTypeIds.set(normalizedName, new Set());
        }
        nameToTypeIds.get(normalizedName).add(numericTypeId);
    };

    const walkTree = (nodes) => {
        (Array.isArray(nodes) ? nodes : []).forEach((node) => {
            const typeId = Number(node && (node.type_id || node.typeId)) || 0;
            const typeName = String((node && (node.type_name || node.typeName)) || '');
            addNameType(typeId, typeName);
            const children = Array.isArray(node && node.sub_materials)
                ? node.sub_materials
                : (Array.isArray(node && node.subMaterials) ? node.subMaterials : []);
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

    const grouped = payload.materials_by_group || payload.materialsByGroup || {};
    Object.values(grouped).forEach((group) => {
        if (!group || !Array.isArray(group.items)) {
            return;
        }
        addArrayEntries(group.items);
    });

    ownedByName.forEach((qty, normalizedName) => {
        const typeIds = nameToTypeIds.get(normalizedName);
        if (!typeIds || typeIds.size === 0) {
            unresolvedNames.push(labelsByName.get(normalizedName) || normalizedName);
            return;
        }

        const chosenTypeId = Array.from(typeIds)[0];
        byType.set(chosenTypeId, (byType.get(chosenTypeId) || 0) + qty);
        matchedNames.add(normalizedName);
    });

    return {
        byType,
        matchedNames,
        unresolvedNames,
    };
}

function computeNeededItemsFromTreeWithOwned(api, ownedByType) {
    const payload = window.BLUEPRINT_DATA || {};
    const rootNodes = Array.isArray(payload.materials_tree) ? payload.materials_tree : [];
    if (rootNodes.length === 0) {
        return null;
    }

    const marketGroupMap = payload.market_group_map || {};
    const results = new Map(); // typeId -> { typeId, typeName, quantity, marketGroup }
    const remainingOwned = new Map(ownedByType || []);

    const addResult = (typeId, typeName, marketGroup, qty) => {
        const numericTypeId = Number(typeId) || 0;
        const normalizedQty = Math.max(0, Math.ceil(Number(qty) || 0));
        if (!numericTypeId || normalizedQty <= 0) {
            return;
        }
        const existing = results.get(numericTypeId) || {
            typeId: numericTypeId,
            typeName: String(typeName || ''),
            quantity: 0,
            marketGroup: String(marketGroup || ''),
        };
        existing.quantity += normalizedQty;
        if (!existing.typeName && typeName) {
            existing.typeName = String(typeName);
        }
        if (!existing.marketGroup && marketGroup) {
            existing.marketGroup = String(marketGroup);
        }
        results.set(numericTypeId, existing);
    };

    const resolveSwitchState = (typeId, craftable) => {
        if (!craftable) {
            return 'prod';
        }
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
    };

    const walk = (nodes, multiplier) => {
        (Array.isArray(nodes) ? nodes : []).forEach((node) => {
            const typeId = Number(node && (node.type_id || node.typeId)) || 0;
            if (!typeId) {
                return;
            }

            const rawQty = Number(node && (node.quantity ?? node.qty ?? 0));
            if (!Number.isFinite(rawQty) || rawQty <= 0) {
                return;
            }

            const requiredQty = Math.max(0, Math.ceil(rawQty * multiplier));
            if (requiredQty <= 0) {
                return;
            }

            const typeName = String((node && (node.type_name || node.typeName)) || '');
            const marketGroup = marketGroupMap[typeId] && marketGroupMap[typeId].group_name
                ? String(marketGroupMap[typeId].group_name)
                : '';
            const children = Array.isArray(node && node.sub_materials)
                ? node.sub_materials
                : (Array.isArray(node && node.subMaterials) ? node.subMaterials : []);
            const craftable = children.length > 0;
            const state = resolveSwitchState(typeId, craftable);

            if (state === 'useless') {
                return;
            }

            const ownedQty = Math.max(0, Number(remainingOwned.get(typeId) || 0));
            const consumedQty = Math.min(requiredQty, ownedQty);
            if (consumedQty > 0) {
                remainingOwned.set(typeId, ownedQty - consumedQty);
            }

            const missingQty = requiredQty - consumedQty;
            if (missingQty <= 0) {
                return;
            }

            if (craftable) {
                if (state === 'buy') {
                    addResult(typeId, typeName, marketGroup, missingQty);
                    return;
                }
                const childMultiplier = missingQty / requiredQty;
                walk(children, childMultiplier);
                return;
            }

            addResult(typeId, typeName, marketGroup, missingQty);
        });
    };

    walk(rootNodes, 1);
    return Array.from(results.values());
}

/**
 * Compute needed purchase list based on user selections
 */
function computeNeededPurchases() {
    const tbody = document.querySelector('#needed-table tbody');
    const totalEl = document.querySelector('.purchase-total');
    const ownedInputEl = document.getElementById('ownedMaterialsInput');
    const ownedSummaryEl = document.getElementById('ownedMaterialsSummary');
    if (!tbody) {
        return;
    }

    tbody.innerHTML = '';
    if (totalEl) {
        totalEl.textContent = formatPrice(0);
    }

    const api = window.SimulationAPI;
    if (!api || typeof api.getNeededMaterials !== 'function') {
        return;
    }

    // Resolve owned input to known type IDs from the current payload.
    const ownedData = parseOwnedMaterialsInput(ownedInputEl ? ownedInputEl.value : '');
    const ownedLookup = buildOwnedTypeLookupFromPayload(
        ownedData.byName,
        ownedData.labelsByName
    );

    // Recompute needed materials from the tree with owned quantities applied as supply.
    // This supports:
    // - BUY items: subtract directly
    // - PROD craftables you already built: subtract their downstream component demand
    const treeItems = computeNeededItemsFromTreeWithOwned(api, ownedLookup.byType);
    const usedTreeComputation = Array.isArray(treeItems);
    const items = usedTreeComputation ? treeItems : (api.getNeededMaterials() || []);
    const aggregated = new Map(); // typeId -> { typeId, name, qty, marketGroup }
    items.forEach((item) => {
        const typeId = Number(item.typeId ?? item.type_id) || 0;
        if (!typeId) return;
        const qty = Math.max(0, Math.ceil(Number(item.quantity ?? item.qty ?? 0))) || 0;
        if (!qty) return;
        const name = String(item.typeName || item.type_name || '');
        const marketGroup = String(item.marketGroup || item.market_group || '');
        const existing = aggregated.get(typeId) || { typeId, name, qty: 0, marketGroup };
        existing.qty += qty;
        if (!existing.name && name) existing.name = name;
        if (!existing.marketGroup && marketGroup) existing.marketGroup = marketGroup;
        aggregated.set(typeId, existing);
    });

    const ordering = getDashboardMaterialsOrdering();
    const rows = Array.from(aggregated.values()).sort((a, b) => {
        const typeA = Number(a.typeId) || 0;
        const typeB = Number(b.typeId) || 0;

        const dashboardA = ordering.itemOrder.get(typeA);
        const dashboardB = ordering.itemOrder.get(typeB);
        const groupA = (a.marketGroup || ordering.fallbackGroupName);
        const groupB = (b.marketGroup || ordering.fallbackGroupName);

        const groupIdxA = dashboardA ? dashboardA.groupIdx : (ordering.groupOrder.has(groupA) ? ordering.groupOrder.get(groupA) : Number.POSITIVE_INFINITY);
        const groupIdxB = dashboardB ? dashboardB.groupIdx : (ordering.groupOrder.has(groupB) ? ordering.groupOrder.get(groupB) : Number.POSITIVE_INFINITY);
        if (groupIdxA !== groupIdxB) {
            return groupIdxA - groupIdxB;
        }

        const itemIdxA = dashboardA ? dashboardA.itemIdx : Number.POSITIVE_INFINITY;
        const itemIdxB = dashboardB ? dashboardB.itemIdx : Number.POSITIVE_INFINITY;
        if (itemIdxA !== itemIdxB) {
            return itemIdxA - itemIdxB;
        }

        const groupCmp = String(groupA).localeCompare(String(groupB), undefined, { sensitivity: 'base' });
        if (groupCmp !== 0) {
            return groupCmp;
        }
        return String(a.name).localeCompare(String(b.name), undefined, { sensitivity: 'base' });
    });

    const rowsToDisplay = usedTreeComputation
        ? rows
        : rows
            .map((item) => {
                const normalizedName = normalizeOwnedMaterialName(item.name || String(item.typeId));
                const ownedQty = ownedData.byName.get(normalizedName) || 0;
                const adjustedQty = Math.max(0, Number(item.qty) - ownedQty);
                return {
                    ...item,
                    qty: adjustedQty,
                };
            })
            .filter((item) => item.qty > 0);

    CRAFT_COMPUTED_NEEDED_ROWS = rowsToDisplay.map((item) => ({
        typeId: Number(item.typeId) || 0,
        typeName: item.name || item.typeName || item.type_name || '',
        quantity: Number(item.qty) || 0,
        marketGroup: item.marketGroup || item.market_group || '',
    }));
    const matchedOwnedNames = ownedLookup.matchedNames;
    const unmatchedOwnedNames = ownedLookup.unresolvedNames.slice();

    if (ownedSummaryEl) {
        if (ownedData.byName.size === 0 && ownedData.malformedLines.length === 0) {
            ownedSummaryEl.textContent = '';
        } else {
            const matchedCount = matchedOwnedNames.size;
            const unmatchedCount = unmatchedOwnedNames.length;
            const malformedCount = ownedData.malformedLines.length;
            const malformedSuffix = malformedCount > 0
                ? ` ${__('Ignored malformed lines')}: ${ownedData.malformedLines.join(', ')}.`
                : '';
            const unmatchedSuffix = unmatchedCount > 0
                ? ` ${__('Unmatched items')}: ${unmatchedOwnedNames.join(', ')}.`
                : '';
            ownedSummaryEl.textContent = `${__('Owned materials applied')}: ${matchedCount} ${__('matched')}, ${unmatchedCount} ${__('unmatched')}.${malformedSuffix}${unmatchedSuffix}`;
        }
    }

    const typeIds = rowsToDisplay.map(r => String(r.typeId));

    // Ensure we have fuzzwork prices where possible, but keep real prices as user overrides.
    const ensurePrices = (typeIdsToFetch) => {
        if (!typeIdsToFetch || typeIdsToFetch.length === 0) {
            return Promise.resolve({});
        }
        if (typeof fetchAllPrices !== 'function') {
            return Promise.resolve({});
        }
        return fetchAllPrices(typeIdsToFetch).then((prices) => {
            try {
                typeIdsToFetch.forEach((tid) => {
                    const raw = prices[tid] ?? prices[String(parseInt(tid, 10))];
                    const price = raw != null ? (parseFloat(raw) || 0) : 0;
                    if (price > 0 && api && typeof api.setPrice === 'function') {
                        api.setPrice(tid, 'fuzzwork', price);
                    }
                });
            } catch (e) {
                // ignore
            }
            return prices || {};
        });
    };

    ensurePrices(typeIds).finally(() => {
        let totalCost = 0;
        rowsToDisplay.forEach((item) => {
            const unitInfo = (api && typeof api.getPrice === 'function') ? api.getPrice(item.typeId, 'buy') : { value: 0 };
            const unit = unitInfo && typeof unitInfo.value === 'number' ? unitInfo.value : 0;
            const line = (unit > 0 ? unit : 0) * item.qty;
            totalCost += line;

            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${escapeHtml(item.name || String(item.typeId))}</td>
                <td class="text-end" data-qty="${item.qty}">${formatInteger(item.qty)}</td>
                <td class="text-end">${formatPrice(unit)}</td>
                <td class="text-end">${formatPrice(line)}</td>
            `;
            tbody.appendChild(tr);
        });

        if (totalEl) {
            totalEl.textContent = formatPrice(totalCost);
        }
        if (typeof updateMaterialsTabFromState === 'function') {
            updateMaterialsTabFromState();
        }
    });
}

/**
 * Set configuration values from Django template
 * @param {string} fuzzworkUrl - URL for Fuzzwork API
 * @param {string} productTypeId - Product type ID
 */
function setCraftBPConfig(fuzzworkUrl, productTypeId) {
    CRAFT_BP.fuzzworkUrl = fuzzworkUrl;
    CRAFT_BP.productTypeId = productTypeId;
}

window.updateMaterialsTabFromState = updateMaterialsTabFromState;
window.updateFinancialTabFromState = updateFinancialTabFromState;
window.updateNeededTabFromState = updateNeededTabFromState;
window.getIndustryFeeConfigFromDom = getIndustryFeeConfigFromDom;

// One-time sort for the server-rendered Cycles table on the Build tab.
// This keeps the UI consistent with the dashboard category ordering.
function sortBuildCyclesTable() {
    const buildPane = document.getElementById('build-pane');
    if (!buildPane) {
        return;
    }

    const table = buildPane.querySelector('table');
    const tbody = table ? table.querySelector('tbody') : null;
    if (!tbody) {
        return;
    }

    const rows = Array.from(tbody.querySelectorAll('tr[data-type-id]'));
    if (rows.length === 0) {
        return;
    }

    const productTypeId = getProductTypeIdValue();
    const payload = window.BLUEPRINT_DATA || {};
    const marketGroupMap = payload.market_group_map || {};
    const ordering = getDashboardMaterialsOrdering();

    const groupNameFor = (typeId) => {
        const info = marketGroupMap[String(typeId)] || marketGroupMap[typeId];
        if (info && typeof info === 'object') {
            return info.group_name || info.groupName || ordering.fallbackGroupName;
        }
        return ordering.fallbackGroupName;
    };

    const nameForRow = (row) => {
        const label = row.querySelector('.small.fw-semibold, .small.fw-bold');
        return (label && label.textContent ? label.textContent.trim() : '').toLowerCase();
    };

    const isFinalProductRow = (row) => {
        if (row.classList.contains('table-primary')) {
            return true;
        }
        const tid = Number(row.getAttribute('data-type-id')) || 0;
        return !!(productTypeId && tid === productTypeId);
    };

    const finalRows = rows.filter(isFinalProductRow);
    const otherRows = rows.filter(r => !isFinalProductRow(r));

    otherRows.sort((a, b) => {
        const typeA = Number(a.getAttribute('data-type-id')) || 0;
        const typeB = Number(b.getAttribute('data-type-id')) || 0;
        const groupA = groupNameFor(typeA);
        const groupB = groupNameFor(typeB);

        const hasA = ordering.groupOrder.has(groupA);
        const hasB = ordering.groupOrder.has(groupB);

        if (hasA && hasB) {
            const groupIdxA = ordering.groupOrder.get(groupA);
            const groupIdxB = ordering.groupOrder.get(groupB);
            if (groupIdxA !== groupIdxB) {
                return groupIdxA - groupIdxB;
            }
        } else if (hasA !== hasB) {
            // Known dashboard groups first, then the rest.
            return hasA ? -1 : 1;
        } else {
            // Neither group exists in the dashboard list -> sort groups alphabetically.
            const groupCmp = String(groupA).localeCompare(String(groupB), undefined, { sensitivity: 'base' });
            if (groupCmp !== 0) {
                return groupCmp;
            }
        }

        // If the row type happens to exist in dashboard materials list, keep its exact item order.
        const dashA = ordering.itemOrder.get(typeA);
        const dashB = ordering.itemOrder.get(typeB);
        const itemIdxA = dashA ? dashA.itemIdx : Number.POSITIVE_INFINITY;
        const itemIdxB = dashB ? dashB.itemIdx : Number.POSITIVE_INFINITY;
        if (itemIdxA !== itemIdxB) {
            return itemIdxA - itemIdxB;
        }

        return nameForRow(a).localeCompare(nameForRow(b), undefined, { sensitivity: 'base' });
    });

    // Re-append in desired order.
    finalRows.forEach(r => tbody.appendChild(r));
    otherRows.forEach(r => tbody.appendChild(r));
}

try {
    document.addEventListener('DOMContentLoaded', () => {
        sortBuildCyclesTable();

        const buildTabBtn = document.querySelector('#build-tab-btn');
        if (buildTabBtn) {
            buildTabBtn.addEventListener('shown.bs.tab', () => {
                sortBuildCyclesTable();
            });
        }
    });
} catch (e) {
    // ignore
}
