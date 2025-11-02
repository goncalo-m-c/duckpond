/**
 * Authentication Manager for DuckPond Web UI
 * Handles login, logout, session management, and authentication state
 */

class AuthManager {
    constructor() {
        this.user = null;
        this.storageKey = 'duckpond_user';
        this._loadUserFromStorage();
    }

    /**
     * Load user info from sessionStorage
     * @private
     */
    _loadUserFromStorage() {
        try {
            const userData = sessionStorage.getItem(this.storageKey);
            if (userData) {
                this.user = JSON.parse(userData);
            }
        } catch (error) {
            console.error('Failed to load user from storage:', error);
            sessionStorage.removeItem(this.storageKey);
        }
    }

    /**
     * Save user info to sessionStorage
     * @private
     * @param {Object} user - User information
     */
    _saveUserToStorage(user) {
        try {
            sessionStorage.setItem(this.storageKey, JSON.stringify(user));
            this.user = user;
        } catch (error) {
            console.error('Failed to save user to storage:', error);
        }
    }

    /**
     * Clear user info from sessionStorage
     * @private
     */
    _clearUserFromStorage() {
        sessionStorage.removeItem(this.storageKey);
        this.user = null;
    }

    /**
     * Login with API key
     * @param {string} apiKey - User's API key
     * @param {boolean} rememberMe - Whether to remember the session
     * @returns {Promise<Object>} Login response with user and tenant info
     * @throws {Error} If login fails
     */
    async login(apiKey, rememberMe = false) {
        const response = await fetch('/api/auth/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            credentials: 'include',
            body: JSON.stringify({
                api_key: apiKey,
                remember_me: rememberMe,
            }),
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.detail || 'Login failed');
        }

        this._saveUserToStorage({
            account_id: data.user.account_id,
            name: data.user.name,
            tenant: data.tenant,
        });

        return data;
    }

    /**
     * Logout current user
     * @returns {Promise<void>}
     */
    async logout() {
        try {
            await fetch('/api/auth/logout', {
                method: 'POST',
                credentials: 'include',
            });
        } catch (error) {
            console.error('Logout request failed:', error);
        } finally {
            this._clearUserFromStorage();
        }
    }

    /**
     * Get current user information from the server
     * @returns {Promise<Object>} Current user info
     * @throws {Error} If not authenticated or request fails
     */
    async getCurrentUser() {
        const response = await fetch('/api/auth/me', {
            method: 'GET',
            credentials: 'include',
        });

        if (!response.ok) {
            if (response.status === 401) {
                this._clearUserFromStorage();
                throw new Error('Not authenticated');
            }
            throw new Error('Failed to get user information');
        }

        const data = await response.json();
        
        if (this.user) {
            this.user.quotas = data.quotas;
            this.user.api_keys = data.api_keys;
            this._saveUserToStorage(this.user);
        }

        return data;
    }

    /**
     * Check if user is authenticated (has session data)
     * @returns {boolean} True if user appears to be authenticated
     */
    isAuthenticated() {
        return this.user !== null;
    }

    /**
     * Get cached user information
     * @returns {Object|null} User information or null
     */
    getUser() {
        return this.user;
    }

    /**
     * Redirect to login page
     * @param {string} returnUrl - URL to return to after login
     */
    redirectToLogin(returnUrl = null) {
        const currentPath = returnUrl || window.location.pathname;
        if (currentPath !== '/login' && currentPath !== '/') {
            sessionStorage.setItem('duckpond_return_url', currentPath);
        }
        window.location.href = '/login';
    }

    /**
     * Get and clear return URL after login
     * @returns {string} Return URL or default '/app'
     */
    getReturnUrl() {
        const returnUrl = sessionStorage.getItem('duckpond_return_url') || '/app';
        sessionStorage.removeItem('duckpond_return_url');
        return returnUrl;
    }

    /**
     * Verify authentication and redirect if not authenticated
     * Call this on protected pages
     * @returns {Promise<boolean>} True if authenticated, never returns false (redirects instead)
     */
    async requireAuth() {
        if (!this.isAuthenticated()) {
            this.redirectToLogin();
            return false;
        }

        try {
            await this.getCurrentUser();
            return true;
        } catch (error) {
            console.error('Authentication verification failed:', error);
            this.redirectToLogin();
            return false;
        }
    }

    /**
     * Handle 401 Unauthorized responses
     * Clears session and redirects to login
     */
    handle401() {
        this._clearUserFromStorage();
        this.redirectToLogin();
    }
}

/**
 * Global fetch wrapper that handles 401 responses
 * @param {string} url - URL to fetch
 * @param {Object} options - Fetch options
 * @returns {Promise<Response>} Fetch response
 */
async function authenticatedFetch(url, options = {}) {
    const response = await fetch(url, {
        ...options,
        credentials: 'include',
    });

    if (response.status === 401) {
        const authManager = window.authManager || new AuthManager();
        authManager.handle401();
        throw new Error('Authentication required');
    }

    return response;
}

// Create global auth manager instance
if (typeof window !== 'undefined') {
    window.authManager = new AuthManager();
    window.authenticatedFetch = authenticatedFetch;
}

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { AuthManager, authenticatedFetch };
}
