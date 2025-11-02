/**
 * Utility functions for DuckPond Web UI
 */

/**
 * Format bytes to human readable string
 * @param {number} bytes - Bytes to format
 * @param {number} decimals - Number of decimal places
 * @returns {string} Formatted string (e.g., "1.5 GB")
 */
function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return "0 Bytes";

  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + " " + sizes[i];
}

/**
 * Format date to relative time string
 * @param {Date|string|number} date - Date to format (Date object, ISO string, or Unix timestamp)
 * @returns {string} Relative time (e.g., "2 hours ago")
 */
function formatRelativeTime(date) {
  // Handle null, undefined, or empty values
  if (!date && date !== 0) {
    return "Unknown";
  }

  let d;

  // Handle Unix timestamp (number)
  if (typeof date === "number") {
    // Unix timestamps are in seconds, JavaScript Date expects milliseconds
    d = new Date(date * 1000);
  } else if (typeof date === "string") {
    d = new Date(date);
  } else {
    d = date;
  }

  // Check if date is valid
  if (isNaN(d.getTime())) {
    return "Unknown";
  }

  const now = new Date();
  const seconds = Math.floor((now - d) / 1000);

  // Handle future dates
  if (seconds < 0) {
    return "just now";
  }

  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} minute${minutes > 1 ? "s" : ""} ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} hour${hours > 1 ? "s" : ""} ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days} day${days > 1 ? "s" : ""} ago`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months} month${months > 1 ? "s" : ""} ago`;
  const years = Math.floor(months / 12);
  return `${years} year${years > 1 ? "s" : ""} ago`;
}

/**
 * Format date to locale string
 * @param {Date|string|number} date - Date to format (Date object, ISO string, or Unix timestamp)
 * @returns {string} Formatted date
 */
function formatDate(date) {
  // Handle null, undefined, or empty values
  if (!date && date !== 0) {
    return "Unknown";
  }

  let d;

  // Handle Unix timestamp (number)
  if (typeof date === "number") {
    d = new Date(date * 1000);
  } else if (typeof date === "string") {
    d = new Date(date);
  } else {
    d = date;
  }

  // Check if date is valid
  if (isNaN(d.getTime())) {
    return "Unknown";
  }

  return d.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

/**
 * Format date and time to locale string
 * @param {Date|string|number} date - Date to format (Date object, ISO string, or Unix timestamp)
 * @returns {string} Formatted date and time
 */
function formatDateTime(date) {
  // Handle null, undefined, or empty values
  if (!date && date !== 0) {
    return "Unknown";
  }

  let d;

  // Handle Unix timestamp (number)
  if (typeof date === "number") {
    d = new Date(date * 1000);
  } else if (typeof date === "string") {
    d = new Date(date);
  } else {
    d = date;
  }

  // Check if date is valid
  if (isNaN(d.getTime())) {
    return "Unknown";
  }

  return d.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

/**
 * Debounce function execution
 * @param {Function} func - Function to debounce
 * @param {number} wait - Milliseconds to wait
 * @returns {Function} Debounced function
 */
function debounce(func, wait) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}

/**
 * Throttle function execution
 * @param {Function} func - Function to throttle
 * @param {number} limit - Milliseconds between calls
 * @returns {Function} Throttled function
 */
function throttle(func, limit) {
  let inThrottle;
  return function (...args) {
    if (!inThrottle) {
      func.apply(this, args);
      inThrottle = true;
      setTimeout(() => (inThrottle = false), limit);
    }
  };
}

/**
 * Copy text to clipboard
 * @param {string} text - Text to copy
 * @returns {Promise<void>}
 */
async function copyToClipboard(text) {
  if (navigator.clipboard) {
    await navigator.clipboard.writeText(text);
  } else {
    // Fallback for older browsers
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand("copy");
    document.body.removeChild(textarea);
  }
}

/**
 * Show toast notification
 * @param {string} message - Message to display
 * @param {string} type - Type of toast (success, error, info, warning)
 * @param {number} duration - Duration in milliseconds
 */
function showToast(message, type = "info", duration = 3000) {
  // Use global toast manager if available
  if (typeof window !== "undefined" && window.toast) {
    return window.toast.show(message, type, duration);
  }
  // Fallback to console
  console.log(`[${type.toUpperCase()}] ${message}`);
}

/**
 * Parse query string to object
 * @param {string} queryString - Query string to parse
 * @returns {Object} Parsed query parameters
 */
function parseQueryString(queryString) {
  const params = new URLSearchParams(queryString);
  const result = {};
  for (const [key, value] of params) {
    result[key] = value;
  }
  return result;
}

/**
 * Build query string from object
 * @param {Object} params - Parameters to encode
 * @returns {string} Query string
 */
function buildQueryString(params) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== null && value !== undefined) {
      searchParams.append(key, value);
    }
  }
  return searchParams.toString();
}

// Export for module systems
if (typeof window !== "undefined") {
  window.utils = {
    formatBytes,
    formatRelativeTime,
    formatDate,
    formatDateTime,
    debounce,
    throttle,
    copyToClipboard,
    showToast,
    parseQueryString,
    buildQueryString,
  };
}
