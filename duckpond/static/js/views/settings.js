/**
 * Settings View
 * Displays tenant info, API keys, usage & quotas
 */

class SettingsView {
  constructor() {
    this.container = null;
    this.data = {
      account: null,
      apiKeys: [],
      usage: {},
      quotas: {},
    };
  }

  /**
   * Render the settings view
   * @param {HTMLElement} container - Container element
   */
  async render(container) {
    this.container = container;

    // Show loading state
    this._showLoading();

    try {
      // Fetch settings data
      await this._fetchData();

      // Render view
      this._renderSettings();
    } catch (error) {
      console.error("Settings render error:", error);
      this._renderError(error);
    }
  }

  /**
   * Cleanup when leaving view
   */
  destroy() {
    // No polling needed for settings
  }

  /**
   * Fetch settings data
   * @private
   */
  async _fetchData() {
    try {
      // Get account info from new endpoint
      const accountData = await window.api.get("/api/accounts/me");

      // Extract data
      this.data.account = accountData.account;
      this.data.apiKeys = accountData.api_keys || [];
      this.data.quotas = accountData.quotas || {};
      this.data.usage = accountData.usage || {};

      // Calculate additional usage (notebooks count)
      await this._calculateUsage();
    } catch (error) {
      console.error("Failed to fetch settings data:", error);
      throw error;
    }
  }

  /**
   * Calculate usage statistics
   * @private
   */
  async _calculateUsage() {
    try {
      // Fetch notebooks for count
      const notebooks = await window.api.listNotebooks();
      const notebooksArray = Array.isArray(notebooks) ? notebooks : [];

      // Fetch sessions for active count
      const sessions = await window.api.listSessions();
      const sessionsArray = Array.isArray(sessions) ? sessions : [];

      // Update usage with calculated values
      this.data.usage = {
        ...this.data.usage,
        notebooks: notebooksArray.length,
        active_sessions: sessionsArray.length,
      };
    } catch (error) {
      console.warn("Failed to calculate usage:", error);
      // Keep existing usage data from API
    }
  }

  /**
   * Show loading state
   * @private
   */
  _showLoading() {
    this.container.innerHTML = `
            <div class="loading-screen">
                <div class="loading-spinner loading-spinner-large"></div>
                <p class="loading-text">Loading settings...</p>
            </div>
        `;
  }

  /**
   * Show error state
   * @private
   */
  _renderError(error) {
    this.container.innerHTML = "";
    const errorDisplay = ErrorDisplay.create(error, {
      title: "Failed to load settings",
      showRetry: true,
      onRetry: () => {
        this.render(this.container);
      },
    });
    this.container.appendChild(errorDisplay);
    toast.error("Failed to load settings");
  }

  /**
   * Render settings view
   * @private
   */
  _renderSettings() {
    const account = this.data.account;
    const apiKeys = this.data.apiKeys;
    const usage = this.data.usage;
    const quotas = this.data.quotas;

    this.container.innerHTML = `
            <div class="settings-view">
                <div class="settings-header">
                    <h1>Settings</h1>
                    <p class="text-muted">Manage your account and API keys</p>
                </div>

                <!-- Tenant Information -->
                <div class="settings-section">
                    <h2>Tenant Information</h2>
                    <div class="settings-card">
                        <div class="settings-row">
                            <div class="settings-label">Account ID</div>
                            <div class="settings-value">
                                <code>${account?.account_id || "N/A"}</code>
                                <button class="btn-icon-small" onclick="navigator.clipboard.writeText('${account?.account_id}')" title="Copy to clipboard">
                                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                        <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                                        <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                                    </svg>
                                </button>
                            </div>
                        </div>
                        <div class="settings-row">
                            <div class="settings-label">Name</div>
                            <div class="settings-value">${account?.name || "N/A"}</div>
                        </div>
                        <div class="settings-row">
                            <div class="settings-label">Storage Backend</div>
                            <div class="settings-value">${account?.storage_backend || "N/A"}</div>
                        </div>
                        <div class="settings-row">
                            <div class="settings-label">Status</div>
                            <div class="settings-value">
                                <span class="status-badge status-active">● Active</span>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- API Keys -->
                <div class="settings-section">
                    <div class="section-header-row">
                        <h2>API Keys</h2>
                        <button class="btn-primary btn-sm" id="create-api-key-btn">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <line x1="12" y1="5" x2="12" y2="19"></line>
                                <line x1="5" y1="12" x2="19" y2="12"></line>
                            </svg>
                            New API Key
                        </button>
                    </div>
                    <div class="settings-card">
                        ${apiKeys.length > 0 ? this._renderApiKeys() : this._renderNoApiKeys()}
                    </div>
                </div>

                <!-- Usage & Quotas -->
                <div class="settings-section">
                    <h2>Usage & Quotas</h2>
                    <div class="settings-card">
                        ${this._renderUsageQuotas()}
                    </div>
                </div>
            </div>

            <!-- Create API Key Modal -->
            <div class="modal" id="create-key-modal">
                <div class="modal-overlay"></div>
                <div class="modal-content">
                    <div class="modal-header">
                        <h2>Create New API Key</h2>
                        <button class="modal-close" id="modal-close-create">
                            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <line x1="18" y1="6" x2="6" y2="18"></line>
                                <line x1="6" y1="6" x2="18" y2="18"></line>
                            </svg>
                        </button>
                    </div>
                    <div class="modal-body">
                        <div class="form-group">
                            <label for="key-name">Key Name (Optional)</label>
                            <input
                                type="text"
                                id="key-name"
                                placeholder="My API Key"
                                autocomplete="off"
                            />
                            <span class="input-hint">Give your key a memorable name</span>
                        </div>
                        <div id="create-key-error" class="error-message hidden"></div>
                    </div>
                    <div class="modal-footer">
                        <button class="btn-secondary" id="modal-cancel-create">Cancel</button>
                        <button class="btn-primary" id="modal-create-key">Create Key</button>
                    </div>
                </div>
            </div>

            <!-- Show API Key Modal -->
            <div class="modal" id="show-key-modal">
                <div class="modal-overlay"></div>
                <div class="modal-content">
                    <div class="modal-header">
                        <h2>New API Key Created</h2>
                        <button class="modal-close" id="modal-close-show">
                            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <line x1="18" y1="6" x2="6" y2="18"></line>
                                <line x1="6" y1="6" x2="18" y2="18"></line>
                            </svg>
                        </button>
                    </div>
                    <div class="modal-body">
                        <div class="warning-box">
                            <strong>⚠️ Important:</strong> Copy this key now. You won't be able to see it again!
                        </div>
                        <div class="key-display">
                            <code id="new-api-key"></code>
                            <button class="btn-icon" id="copy-key-btn" title="Copy to clipboard">
                                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                                    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                                </svg>
                            </button>
                        </div>
                        <p class="text-muted text-small">Store this key securely. It provides full access to your account.</p>
                    </div>
                    <div class="modal-footer">
                        <button class="btn-primary" id="modal-done-show">Done</button>
                    </div>
                </div>
            </div>

            <!-- Revoke API Key Modal -->
            <div class="modal" id="revoke-key-modal">
                <div class="modal-overlay"></div>
                <div class="modal-content">
                    <div class="modal-header">
                        <h2>Revoke API Key</h2>
                        <button class="modal-close" id="modal-close-revoke">
                            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <line x1="18" y1="6" x2="6" y2="18"></line>
                                <line x1="6" y1="6" x2="18" y2="18"></line>
                            </svg>
                        </button>
                    </div>
                    <div class="modal-body">
                        <p>Are you sure you want to revoke this API key?</p>
                        <div class="key-info">
                            <strong id="revoke-key-preview"></strong>
                        </div>
                        <p class="text-muted">Applications using this key will immediately lose access.</p>
                    </div>
                    <div class="modal-footer">
                        <button class="btn-secondary" id="modal-cancel-revoke">Cancel</button>
                        <button class="btn-danger" id="modal-confirm-revoke">Revoke Key</button>
                    </div>
                </div>
            </div>
        `;

    // Attach event listeners
    this._attachEventListeners();
  }

  /**
   * Render API keys list
   * @private
   */
  _renderApiKeys() {
    return this.data.apiKeys
      .map(
        (key) => `
            <div class="api-key-row">
                <div class="api-key-info">
                    <div class="api-key-preview">
                        <code>${key.key_preview || key.key_id}</code>
                    </div>
                    <div class="api-key-meta">
                        <span>Created ${window.utils.formatRelativeTime(key.created_at)}</span>
                        ${
                          key.last_used
                            ? `
                            <span>•</span>
                            <span>Last used ${window.utils.formatRelativeTime(key.last_used)}</span>
                        `
                            : ""
                        }
                        ${
                          key.expires_at
                            ? `
                            <span>•</span>
                            <span>Expires ${window.utils.formatDate(key.expires_at)}</span>
                        `
                            : ""
                        }
                    </div>
                </div>
                <button
                    class="btn-danger-outline btn-sm"
                    data-action="revoke-key"
                    data-key-id="${key.key_id}"
                    data-key-preview="${key.key_preview || key.key_id}"
                >
                    Revoke
                </button>
            </div>
        `,
      )
      .join("");
  }

  /**
   * Render no API keys message
   * @private
   */
  _renderNoApiKeys() {
    return `
            <div class="empty-message">
                <p>No API keys found. Create your first API key to get started.</p>
            </div>
        `;
  }

  /**
   * Render usage and quotas
   * @private
   */
  _renderUsageQuotas() {
    const usage = this.data.usage;
    const quotas = this.data.quotas;

    // Calculate percentages
    const notebooksPercent = quotas.max_notebooks
      ? ((usage.notebooks || 0) / quotas.max_notebooks) * 100
      : 0;
    const storagePercent = quotas.max_storage_gb
      ? ((usage.storage_gb || 0) / quotas.max_storage_gb) * 100
      : 0;
    const sessionsPercent = quotas.max_concurrent_queries
      ? ((usage.active_sessions || 0) / quotas.max_concurrent_queries) * 100
      : 0;

    return `
            <div class="quota-item">
                <div class="quota-header">
                    <span class="quota-label">Notebooks</span>
                    <span class="quota-value">${usage.notebooks || 0} / ${quotas.max_notebooks || "∞"}</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: ${Math.min(notebooksPercent, 100)}%"></div>
                </div>
            </div>

            <div class="quota-item">
                <div class="quota-header">
                    <span class="quota-label">Storage</span>
                    <span class="quota-value">${(usage.storage_gb || 0).toFixed(2)} GB / ${quotas.max_storage_gb || "∞"} GB</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: ${Math.min(storagePercent, 100)}%"></div>
                </div>
            </div>

            <div class="quota-item">
                <div class="quota-header">
                    <span class="quota-label">Active Sessions</span>
                    <span class="quota-value">${usage.active_sessions || 0} / ${quotas.max_concurrent_queries || "∞"}</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: ${Math.min(sessionsPercent, 100)}%"></div>
                </div>
            </div>

            <div class="quota-item">
                <div class="quota-header">
                    <span class="quota-label">Query Memory</span>
                    <span class="quota-value">${quotas.max_query_memory_gb || 0} GB</span>
                </div>
            </div>
        `;
  }

  /**
   * Attach event listeners
   * @private
   */
  _attachEventListeners() {
    // Create API key button
    const createBtn = this.container.querySelector("#create-api-key-btn");
    if (createBtn) {
      createBtn.addEventListener("click", () => this._showCreateModal());
    }

    // Revoke buttons
    const revokeButtons = this.container.querySelectorAll(
      '[data-action="revoke-key"]',
    );
    revokeButtons.forEach((btn) => {
      btn.addEventListener("click", () => {
        const keyId = btn.dataset.keyId;
        const keyPreview = btn.dataset.keyPreview;
        this._showRevokeModal(keyId, keyPreview);
      });
    });

    // Modal listeners
    this._attachModalListeners();
  }

  /**
   * Attach modal listeners
   * @private
   */
  _attachModalListeners() {
    // Create modal
    const createModal = this.container.querySelector("#create-key-modal");
    const closeCreate = this.container.querySelector("#modal-close-create");
    const cancelCreate = this.container.querySelector("#modal-cancel-create");
    const confirmCreate = this.container.querySelector("#modal-create-key");
    const keyNameInput = this.container.querySelector("#key-name");

    if (closeCreate)
      closeCreate.addEventListener("click", () =>
        this._hideModal("create-key"),
      );
    if (cancelCreate)
      cancelCreate.addEventListener("click", () =>
        this._hideModal("create-key"),
      );
    if (confirmCreate)
      confirmCreate.addEventListener("click", () => this._createApiKey());
    if (keyNameInput) {
      keyNameInput.addEventListener("keypress", (e) => {
        if (e.key === "Enter") this._createApiKey();
      });
    }

    // Show key modal
    const closeShow = this.container.querySelector("#modal-close-show");
    const doneShow = this.container.querySelector("#modal-done-show");
    const copyKeyBtn = this.container.querySelector("#copy-key-btn");

    if (closeShow)
      closeShow.addEventListener("click", () => this._hideModal("show-key"));
    if (doneShow)
      doneShow.addEventListener("click", () => this._hideModal("show-key"));
    if (copyKeyBtn) {
      copyKeyBtn.addEventListener("click", async () => {
        const keyElement = this.container.querySelector("#new-api-key");
        if (keyElement) {
          await window.utils.copyToClipboard(keyElement.textContent);
          copyKeyBtn.innerHTML = "✓ Copied";
          setTimeout(() => {
            copyKeyBtn.innerHTML = `
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                                <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                            </svg>
                        `;
          }, 2000);
        }
      });
    }

    // Revoke modal
    const closeRevoke = this.container.querySelector("#modal-close-revoke");
    const cancelRevoke = this.container.querySelector("#modal-cancel-revoke");
    const confirmRevoke = this.container.querySelector("#modal-confirm-revoke");

    if (closeRevoke)
      closeRevoke.addEventListener("click", () =>
        this._hideModal("revoke-key"),
      );
    if (cancelRevoke)
      cancelRevoke.addEventListener("click", () =>
        this._hideModal("revoke-key"),
      );
    if (confirmRevoke)
      confirmRevoke.addEventListener("click", () => this._revokeApiKey());

    // Click outside to close
    const modals = this.container.querySelectorAll(".modal");
    modals.forEach((modal) => {
      const overlay = modal.querySelector(".modal-overlay");
      if (overlay) {
        overlay.addEventListener("click", () => {
          const modalId = modal.id;
          this._hideModal(modalId);
        });
      }
    });
  }

  /**
   * Show create API key modal
   * @private
   */
  _showCreateModal() {
    const modal = this.container.querySelector("#create-key-modal");
    const input = this.container.querySelector("#key-name");
    const error = this.container.querySelector("#create-key-error");

    if (modal) {
      modal.classList.add("active");
      if (input) {
        input.value = "";
        input.focus();
      }
      if (error) {
        error.classList.add("hidden");
      }
    }
  }

  /**
   * Show revoke modal
   * @private
   */
  _showRevokeModal(keyId, keyPreview) {
    // Use new Modal component
    Modal.confirm({
      title: "Revoke API Key?",
      message: `Revoke API key "${keyPreview}"? Applications using this key will lose access immediately.`,
      type: "danger",
      confirmText: "Revoke",
      cancelText: "Cancel",
      onConfirm: async () => {
        try {
          await window.api.delete(`/api/accounts/me/api-keys/${keyId}`);

          // Refresh data
          await this._fetchData();
          this._renderSettings();

          toast.success("API key revoked successfully");
        } catch (error) {
          console.error("Failed to revoke API key:", error);
          toast.error("Failed to revoke API key: " + error.message);
        }
      },
    });
  }

  /**
   * Hide modal
   * @private
   */
  _hideModal(modalId) {
    const modal = this.container.querySelector(`#${modalId}-modal`);
    if (modal) {
      modal.classList.remove("active");
    }
  }

  /**
   * Create API key
   * @private
   */
  async _createApiKey() {
    const input = this.container.querySelector("#key-name");
    const createBtn = this.container.querySelector("#modal-create-key");
    const error = this.container.querySelector("#create-key-error");

    const name = input.value.trim();

    // Show loading state
    ButtonLoadingState.show(createBtn, "Creating...");

    try {
      // Call API to create new key
      const response = await window.api.post("/api/accounts/me/api-keys", {
        name: name || null,
      });

      // Show the new key
      this._showNewKey(response.api_key);

      // Hide create modal
      this._hideModal("create-key");

      // Refresh data
      await this._fetchData();
      this._renderSettings();

      toast.success("API key created! Make sure to copy it now.");
    } catch (error) {
      console.error("Failed to create API key:", error);
      if (error) {
        error.textContent = error.message || "Failed to create API key";
        error.classList.remove("hidden");
      }
      toast.error("Failed to create API key");
    } finally {
      ButtonLoadingState.hide(createBtn);
    }
  }

  /**
   * Show new key modal
   * @private
   */
  _showNewKey(key) {
    const modal = this.container.querySelector("#show-key-modal");
    const keyElement = this.container.querySelector("#new-api-key");
    if (modal && keyElement) {
      keyElement.textContent = key;
      modal.classList.add("active");
    }

    // Also show a toast for quick feedback
    toast.success("API key created! Make sure to copy it now.");
  }

  /**
   * Revoke API key (legacy method - now using Modal.confirm in _showRevokeModal)
   * @private
   */
  async _revokeApiKey() {
    const confirmBtn = this.container.querySelector("#modal-confirm-revoke");
    if (!confirmBtn) return;

    const keyId = confirmBtn.dataset.keyId;

    ButtonLoadingState.show(confirmBtn, "Revoking...");

    try {
      await window.api.delete(`/api/accounts/me/api-keys/${keyId}`);

      // Hide modal
      this._hideModal("revoke-key");

      // Refresh data
      await this._fetchData();
      this._renderSettings();

      toast.success("API key revoked successfully");
    } catch (error) {
      console.error("Failed to revoke API key:", error);
      toast.error("Failed to revoke API key: " + error.message);
    } finally {
      ButtonLoadingState.hide(confirmBtn);
    }
  }
}

// Export for use in app.js
if (typeof window !== "undefined") {
  window.SettingsView = SettingsView;
}
