/**
 * UI Components for DuckPond Web UI
 * Includes: Loading States, Toast Notifications, Modals, Empty States
 */

// ============================================================================
// Toast Notifications
// ============================================================================

class ToastManager {
  constructor() {
    this.container = null;
    this.toasts = new Map();
    this.init();
  }

  init() {
    // Create toast container if it doesn't exist
    if (!document.getElementById("toast-container")) {
      this.container = document.createElement("div");
      this.container.id = "toast-container";
      this.container.className = "toast-container";
      document.body.appendChild(this.container);
    } else {
      this.container = document.getElementById("toast-container");
    }
  }

  show(message, type = "info", duration = 3000) {
    const id = Date.now() + Math.random();
    const toast = this.createToast(id, message, type);

    this.container.appendChild(toast);
    this.toasts.set(id, toast);

    // Trigger animation
    requestAnimationFrame(() => {
      toast.classList.add("show");
    });

    // Auto-dismiss
    if (duration > 0) {
      setTimeout(() => this.dismiss(id), duration);
    }

    return id;
  }

  createToast(id, message, type) {
    const toast = document.createElement("div");
    toast.className = `toast toast-${type}`;
    toast.dataset.toastId = id;

    const icon = this.getIcon(type);

    toast.innerHTML = `
            <div class="toast-content">
                <div class="toast-icon">${icon}</div>
                <div class="toast-message">${message}</div>
                <button class="toast-close" aria-label="Close">
                    <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                        <path d="M12 4L4 12M4 4L12 12" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                    </svg>
                </button>
            </div>
        `;

    const closeBtn = toast.querySelector(".toast-close");
    closeBtn.addEventListener("click", () => this.dismiss(id));

    return toast;
  }

  getIcon(type) {
    const icons = {
      success: `<svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                <circle cx="10" cy="10" r="9" stroke="currentColor" stroke-width="2"/>
                <path d="M6 10L9 13L14 7" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>`,
      error: `<svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                <circle cx="10" cy="10" r="9" stroke="currentColor" stroke-width="2"/>
                <path d="M10 6V10M10 14H10.01" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
            </svg>`,
      warning: `<svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                <path d="M10 2L2 17H18L10 2Z" stroke="currentColor" stroke-width="2" stroke-linejoin="round"/>
                <path d="M10 8V11M10 14H10.01" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
            </svg>`,
      info: `<svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                <circle cx="10" cy="10" r="9" stroke="currentColor" stroke-width="2"/>
                <path d="M10 10V14M10 6H10.01" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
            </svg>`,
    };
    return icons[type] || icons.info;
  }

  dismiss(id) {
    const toast = this.toasts.get(id);
    if (!toast) return;

    toast.classList.remove("show");
    toast.classList.add("hide");

    setTimeout(() => {
      if (toast.parentNode) {
        toast.parentNode.removeChild(toast);
      }
      this.toasts.delete(id);
    }, 300);
  }

  success(message, duration = 3000) {
    return this.show(message, "success", duration);
  }

  error(message, duration = 5000) {
    return this.show(message, "error", duration);
  }

  warning(message, duration = 4000) {
    return this.show(message, "warning", duration);
  }

  info(message, duration = 3000) {
    return this.show(message, "info", duration);
  }
}

// Global toast instance
const toast = new ToastManager();

// ============================================================================
// Loading States
// ============================================================================

class LoadingSpinner {
  static create(size = "medium") {
    const spinner = document.createElement("div");
    spinner.className = `loading-spinner loading-spinner-${size}`;
    return spinner;
  }

  static createWithText(text, size = "medium") {
    const container = document.createElement("div");
    container.className = "loading-container";
    container.innerHTML = `
            <div class="loading-spinner loading-spinner-${size}"></div>
            <p class="loading-text">${text}</p>
        `;
    return container;
  }

  static createFullScreen(text = "Loading...") {
    const overlay = document.createElement("div");
    overlay.className = "loading-overlay";
    overlay.innerHTML = `
            <div class="loading-content">
                <div class="loading-spinner loading-spinner-large"></div>
                <p class="loading-text">${text}</p>
            </div>
        `;
    return overlay;
  }

  static createSkeleton(type = "text") {
    const skeleton = document.createElement("div");
    skeleton.className = `skeleton skeleton-${type}`;
    return skeleton;
  }
}

class ButtonLoadingState {
  static show(button, text = null) {
    if (button.dataset.loading === "true") return;

    button.dataset.loading = "true";
    button.dataset.originalText = button.innerHTML;
    button.disabled = true;

    const spinnerHTML = `
            <span class="btn-spinner"></span>
            ${text ? `<span>${text}</span>` : ""}
        `;
    button.innerHTML = spinnerHTML;
  }

  static hide(button) {
    if (button.dataset.loading !== "true") return;

    button.dataset.loading = "false";
    button.disabled = false;
    button.innerHTML = button.dataset.originalText || "";
  }
}

class ProgressIndicator {
  constructor(steps) {
    this.steps = steps;
    this.currentStep = 0;
    this.element = null;
  }

  create() {
    this.element = document.createElement("div");
    this.element.className = "progress-indicator";

    this.element.innerHTML = `
            <div class="progress-steps">
                ${this.steps
                  .map(
                    (step, i) => `
                    <div class="progress-step" data-step="${i}">
                        <div class="progress-step-icon">
                            <span class="step-number">${i + 1}</span>
                            <svg class="step-check" width="16" height="16" viewBox="0 0 16 16" fill="none">
                                <path d="M3 8L6 11L13 4" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                            </svg>
                            <div class="step-spinner"></div>
                        </div>
                        <div class="progress-step-label">${step}</div>
                    </div>
                `,
                  )
                  .join("")}
            </div>
            <div class="progress-bar">
                <div class="progress-bar-fill"></div>
            </div>
        `;

    return this.element;
  }

  setStep(stepIndex) {
    if (!this.element) return;

    this.currentStep = stepIndex;
    const stepElements = this.element.querySelectorAll(".progress-step");
    const progressFill = this.element.querySelector(".progress-bar-fill");

    stepElements.forEach((el, i) => {
      el.classList.remove("active", "completed", "loading");

      if (i < stepIndex) {
        el.classList.add("completed");
      } else if (i === stepIndex) {
        el.classList.add("active", "loading");
      }
    });

    // Update progress bar
    const progress = (stepIndex / (this.steps.length - 1)) * 100;
    progressFill.style.width = `${Math.min(progress, 100)}%`;
  }

  complete() {
    if (!this.element) return;

    const stepElements = this.element.querySelectorAll(".progress-step");
    stepElements.forEach((el) => {
      el.classList.remove("active", "loading");
      el.classList.add("completed");
    });

    const progressFill = this.element.querySelector(".progress-bar-fill");
    progressFill.style.width = "100%";
  }

  error(stepIndex) {
    if (!this.element) return;

    const stepElement = this.element.querySelector(
      `[data-step="${stepIndex}"]`,
    );
    if (stepElement) {
      stepElement.classList.remove("loading", "active");
      stepElement.classList.add("error");
    }
  }
}

// ============================================================================
// Modal Dialog
// ============================================================================

class Modal {
  constructor(options = {}) {
    this.options = {
      title: options.title || "",
      message: options.message || "",
      type: options.type || "info", // info, warning, danger
      confirmText: options.confirmText || "Confirm",
      cancelText: options.cancelText || "Cancel",
      showCancel: options.showCancel !== false,
      onConfirm: options.onConfirm || null,
      onCancel: options.onCancel || null,
      closeOnBackdrop: options.closeOnBackdrop !== false,
    };

    this.element = null;
    this.isOpen = false;
  }

  create() {
    this.element = document.createElement("div");
    this.element.className = "modal-overlay";

    const iconHTML = this.getIcon(this.options.type);

    this.element.innerHTML = `
            <div class="modal modal-${this.options.type}">
                <div class="modal-header">
                    ${iconHTML ? `<div class="modal-icon">${iconHTML}</div>` : ""}
                    <h3 class="modal-title">${this.options.title}</h3>
                </div>
                <div class="modal-body">
                    <p class="modal-message">${this.options.message}</p>
                </div>
                <div class="modal-footer">
                    ${
                      this.options.showCancel
                        ? `
                        <button class="btn-secondary modal-cancel">${this.options.cancelText}</button>
                    `
                        : ""
                    }
                    <button class="btn-primary modal-confirm">${this.options.confirmText}</button>
                </div>
            </div>
        `;

    return this.element;
  }

  getIcon(type) {
    const icons = {
      warning: `<svg width="24" height="24" viewBox="0 0 24 24" fill="none">
                <path d="M12 2L2 20H22L12 2Z" stroke="currentColor" stroke-width="2" stroke-linejoin="round"/>
                <path d="M12 9V13M12 17H12.01" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
            </svg>`,
      danger: `<svg width="24" height="24" viewBox="0 0 24 24" fill="none">
                <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"/>
                <path d="M15 9L9 15M9 9L15 15" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
            </svg>`,
      info: `<svg width="24" height="24" viewBox="0 0 24 24" fill="none">
                <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"/>
                <path d="M12 12V16M12 8H12.01" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
            </svg>`,
    };
    return icons[type] || "";
  }

  open() {
    console.log("Modal.open() called, isOpen:", this.isOpen);
    if (this.isOpen) return;

    if (!this.element) {
      console.log("Creating modal element...");
      this.create();
    }

    console.log("Appending modal to body...");
    document.body.appendChild(this.element);
    this.isOpen = true;

    // Trigger animation
    requestAnimationFrame(() => {
      console.log("Adding active class to modal...");
      this.element.classList.add("active");
    });

    this.attachListeners();
    this.trapFocus();
  }

  close() {
    if (!this.isOpen || !this.element) return;

    this.element.classList.remove("active");

    setTimeout(() => {
      if (this.element && this.element.parentNode) {
        this.element.parentNode.removeChild(this.element);
      }
      this.isOpen = false;
    }, 300);
  }

  attachListeners() {
    const confirmBtn = this.element.querySelector(".modal-confirm");
    const cancelBtn = this.element.querySelector(".modal-cancel");
    const overlay = this.element;

    confirmBtn.addEventListener("click", () => {
      if (this.options.onConfirm) {
        this.options.onConfirm();
      }
      this.close();
    });

    if (cancelBtn) {
      cancelBtn.addEventListener("click", () => {
        if (this.options.onCancel) {
          this.options.onCancel();
        }
        this.close();
      });
    }

    if (this.options.closeOnBackdrop) {
      overlay.addEventListener("click", (e) => {
        if (e.target === overlay) {
          if (this.options.onCancel) {
            this.options.onCancel();
          }
          this.close();
        }
      });
    }

    // ESC key to close
    this.escapeHandler = (e) => {
      if (e.key === "Escape") {
        if (this.options.onCancel) {
          this.options.onCancel();
        }
        this.close();
      }
    };
    document.addEventListener("keydown", this.escapeHandler);
  }

  trapFocus() {
    const modalElement = this.element.querySelector(".modal");
    const focusableElements = modalElement.querySelectorAll(
      'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
    );
    const firstFocusable = focusableElements[0];
    const lastFocusable = focusableElements[focusableElements.length - 1];

    firstFocusable?.focus();

    this.tabHandler = (e) => {
      if (e.key !== "Tab") return;

      if (e.shiftKey) {
        if (document.activeElement === firstFocusable) {
          lastFocusable?.focus();
          e.preventDefault();
        }
      } else {
        if (document.activeElement === lastFocusable) {
          firstFocusable?.focus();
          e.preventDefault();
        }
      }
    };

    document.addEventListener("keydown", this.tabHandler);
  }

  destroy() {
    if (this.escapeHandler) {
      document.removeEventListener("keydown", this.escapeHandler);
    }
    if (this.tabHandler) {
      document.removeEventListener("keydown", this.tabHandler);
    }
    this.close();
  }

  // Static helper methods
  static confirm(options) {
    console.log("Modal.confirm called with options:", options);
    const modal = new Modal({
      ...options,
      type: options.type || "warning",
    });
    console.log("Modal created, calling open()...");
    modal.open();
    console.log("Modal opened, element:", modal.element);
    return modal;
  }

  static alert(title, message, type = "info") {
    const modal = new Modal({
      title,
      message,
      type,
      showCancel: false,
      confirmText: "OK",
    });
    modal.open();
    return modal;
  }
}

// ============================================================================
// Empty State Component
// ============================================================================

class EmptyState {
  static create(options = {}) {
    const container = document.createElement("div");
    container.className = "empty-state";

    const icon = options.icon || "📭";
    const title = options.title || "No items found";
    const message = options.message || "";
    const actionText = options.actionText || null;
    const actionLink = options.actionLink || null;

    container.innerHTML = `
            <div class="empty-state-content">
                <div class="empty-state-icon">${icon}</div>
                <h3 class="empty-state-title">${title}</h3>
                ${message ? `<p class="empty-state-message">${message}</p>` : ""}
                ${
                  actionText && actionLink
                    ? `
                    <a href="${actionLink}" class="btn-primary empty-state-action">
                        ${actionText}
                    </a>
                `
                    : ""
                }
            </div>
        `;

    return container;
  }
}

// ============================================================================
// Error Display Component
// ============================================================================

class ErrorDisplay {
  static create(error, options = {}) {
    const container = document.createElement("div");
    container.className = `error-display error-display-${options.type || "inline"}`;

    const title = options.title || "Something went wrong";
    const showRetry = options.showRetry !== false;
    const onRetry = options.onRetry || null;

    let errorMessage = "An unexpected error occurred";
    if (typeof error === "string") {
      errorMessage = error;
    } else if (error?.message) {
      errorMessage = error.message;
    }

    container.innerHTML = `
            <div class="error-content">
                <div class="error-icon">
                    <svg width="48" height="48" viewBox="0 0 48 48" fill="none">
                        <circle cx="24" cy="24" r="22" stroke="currentColor" stroke-width="2"/>
                        <path d="M24 14V26M24 34H24.02" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                    </svg>
                </div>
                <h3 class="error-title">${title}</h3>
                <p class="error-message">${errorMessage}</p>
                ${
                  showRetry
                    ? `
                    <button class="btn-primary error-retry">Try Again</button>
                `
                    : ""
                }
            </div>
        `;

    if (showRetry && onRetry) {
      const retryBtn = container.querySelector(".error-retry");
      retryBtn.addEventListener("click", onRetry);
    }

    return container;
  }

  static createInline(message) {
    const error = document.createElement("div");
    error.className = "error-inline";
    error.innerHTML = `
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <circle cx="8" cy="8" r="7" stroke="currentColor" stroke-width="1.5"/>
                <path d="M8 4V8M8 11H8.01" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/>
            </svg>
            <span>${message}</span>
        `;
    return error;
  }
}

// ============================================================================
// Export to window
// ============================================================================

if (typeof window !== "undefined") {
  window.toast = toast;
  window.Toast = ToastManager;
  window.LoadingSpinner = LoadingSpinner;
  window.ButtonLoadingState = ButtonLoadingState;
  window.ProgressIndicator = ProgressIndicator;
  window.Modal = Modal;
  window.EmptyState = EmptyState;
  window.ErrorDisplay = ErrorDisplay;
}
