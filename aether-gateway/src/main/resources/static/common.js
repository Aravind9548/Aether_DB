function armDestructiveAction(btn, config) {
    if (btn.dataset.armed === "true") {
        clearTimeout(Number(btn.dataset.armTimer));
        disarmDestructiveAction(btn, config);
        config.onExecute();
        return;
    }

    btn.dataset.armed = "true";
    btn.textContent = config.confirmText || "Confirm?";
    btn.classList.add(config.armedClass || "confirm-armed");
    btn.dataset.armTimer = setTimeout(() => disarmDestructiveAction(btn, config), config.timeoutMs || 4000);
}

function disarmDestructiveAction(btn, config) {
    btn.dataset.armed = "false";
    btn.textContent = config.defaultText || "DEL";
    btn.classList.remove(config.armedClass || "confirm-armed");
}
