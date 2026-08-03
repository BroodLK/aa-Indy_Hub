/**
 * Buyback navigation loading overlay.
 *
 * Some Buyback pages (sell item list, buy stock browse) need a server round-trip that can
 * take a few seconds, which used to look like a dead click. Any element marked with
 * data-me-loading shows the shared overlay as soon as it is activated:
 *
 *   <a href="..." data-me-loading="Loading Jita 4-4" data-me-loading-detail="Reading your hangars">
 *   <form ... data-me-loading="Submitting sell order">
 *
 * The overlay markup lives in material_exchange/includes/page_loading_overlay.html.
 */

(function() {
    const OVERLAY_ID = 'mePageLoadingOverlay';

    function getOverlay() {
        return document.getElementById(OVERLAY_ID);
    }

    function setText(overlay, selector, text) {
        const node = overlay.querySelector(selector);
        if (node && text) {
            node.textContent = text;
        }
    }

    function showOverlay(title, detail) {
        const overlay = getOverlay();
        if (!overlay) {
            return;
        }
        setText(overlay, '[data-me-loading-title]', title);
        setText(overlay, '[data-me-loading-detail]', detail);
        overlay.classList.remove('d-none');
        overlay.setAttribute('aria-hidden', 'false');
    }

    function hideOverlay() {
        const overlay = getOverlay();
        if (!overlay) {
            return;
        }
        overlay.classList.add('d-none');
        overlay.setAttribute('aria-hidden', 'true');
    }

    function showFromTrigger(trigger) {
        showOverlay(
            trigger.getAttribute('data-me-loading'),
            trigger.getAttribute('data-me-loading-detail')
        );
    }

    /** True for clicks the browser handles itself (new tab/window, download, ...). */
    function isPassthroughClick(event, trigger) {
        if (event.defaultPrevented || event.button !== 0) {
            return true;
        }
        if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
            return true;
        }
        const target = trigger.getAttribute('target');
        if (target && target !== '_self') {
            return true;
        }
        if (trigger.hasAttribute('download')) {
            return true;
        }
        const href = trigger.getAttribute('href');
        if (href !== null && (href === '' || href === '#' || href.startsWith('javascript:'))) {
            return true;
        }
        return false;
    }

    document.addEventListener('click', function(event) {
        const trigger = event.target.closest('[data-me-loading]');
        if (!trigger || trigger.tagName === 'FORM') {
            return;
        }
        if (trigger.disabled || trigger.getAttribute('aria-disabled') === 'true') {
            return;
        }
        if (isPassthroughClick(event, trigger)) {
            return;
        }
        showFromTrigger(trigger);
    });

    document.addEventListener('submit', function(event) {
        const form = event.target.closest('form[data-me-loading]');
        if (!form || event.defaultPrevented) {
            return;
        }
        showFromTrigger(form);
    });

    // Returning through history (including the back/forward cache) must never restore the
    // page with the overlay still covering it.
    window.addEventListener('pageshow', hideOverlay);

    // Escape hatch: if a request dies without navigating, let the user dismiss the overlay.
    document.addEventListener('keydown', function(event) {
        if (event.key === 'Escape') {
            hideOverlay();
        }
    });

    window.indyHubPageLoading = {
        show: showOverlay,
        hide: hideOverlay
    };
})();
