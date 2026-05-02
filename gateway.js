// Complete Password Gateway for GitHub Pages
// Connects to Vercel API for secure password verification

const GATEWAY_CONFIG = {
    // ✅ YOUR VERCELL API URL - UPDATED
    API_URL: 'https://passwordapi-delta.vercel.app/api/verify',
    MAX_ATTEMPTS: 5,
    LOCKOUT_TIME: 900000, // 15 minutes in milliseconds
    SESSION_KEY: 'osint_auth_verified',
    ATTEMPT_KEY: 'osint_auth_attempts',
    LOCKOUT_KEY: 'osint_auth_lockout'
};

// ========== UTILITY FUNCTIONS ==========

function getStoredAttempts() {
    const attempts = sessionStorage.getItem(GATEWAY_CONFIG.ATTEMPT_KEY);
    return attempts ? parseInt(attempts) : 0;
}

function incrementAttempts() {
    const current = getStoredAttempts();
    sessionStorage.setItem(GATEWAY_CONFIG.ATTEMPT_KEY, current + 1);
    return current + 1;
}

function resetAttempts() {
    sessionStorage.removeItem(GATEWAY_CONFIG.ATTEMPT_KEY);
    sessionStorage.removeItem(GATEWAY_CONFIG.LOCKOUT_KEY);
}

function isLockedOut() {
    const lockoutTime = sessionStorage.getItem(GATEWAY_CONFIG.LOCKOUT_KEY);
    if (!lockoutTime) return false;
    
    const timeLeft = parseInt(lockoutTime) - Date.now();
    if (timeLeft > 0) {
        return Math.ceil(timeLeft / 60000); // Return minutes left
    }
    resetAttempts();
    return false;
}

// ========== CORE PASSWORD VERIFICATION ==========

async function verifyPassword(password) {
    try {
        const response = await fetch(GATEWAY_CONFIG.API_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ password: password })
        });
        
        const data = await response.json();
        
        if (response.status === 200 && data.success === true) {
            return { success: true, message: data.message || 'Access granted' };
        } else if (response.status === 401) {
            return { success: false, message: data.message || data.detail || 'Invalid password' };
        } else {
            return { success: false, message: data.message || data.detail || 'Server error. Try again later.' };
        }
    } catch (error) {
        console.error('Verification error:', error);
        return { success: false, message: 'Cannot connect to authentication server. Check your internet.' };
    }
}

// ========== UI COMPONENTS ==========

function createPasswordModal() {
    // Remove any existing modal
    const existingModal = document.getElementById('password-modal');
    if (existingModal) existingModal.remove();
    
    const modal = document.createElement('div');
    modal.id = 'password-modal';
    modal.innerHTML = `
        <div style="
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.95);
            z-index: 9999;
            display: flex;
            justify-content: center;
            align-items: center;
            font-family: 'Courier New', monospace;
        ">
            <div style="
                background: #0a0a0a;
                border: 2px solid #00ff00;
                padding: 40px;
                border-radius: 10px;
                text-align: center;
                max-width: 400px;
                width: 90%;
                box-shadow: 0 0 20px rgba(0,255,0,0.3);
            ">
                <div style="color: #00ff00; font-size: 20px; margin-bottom: 20px;">
                    ⚡ SECURE OSINT TERMINAL ⚡
                </div>
                <div style="color: #fff; margin-bottom: 20px; font-size: 14px;">
                    Enter authorization password
                </div>
                <input type="password" id="password-input" placeholder="Password" style="
                    width: 100%;
                    padding: 12px;
                    background: #1a1a1a;
                    border: 1px solid #00ff00;
                    color: #00ff00;
                    font-family: 'Courier New', monospace;
                    font-size: 16px;
                    margin-bottom: 15px;
                    box-sizing: border-box;
                ">
                <button id="submit-password" style="
                    background: #00ff00;
                    color: #000;
                    border: none;
                    padding: 10px 30px;
                    font-family: 'Courier New', monospace;
                    font-weight: bold;
                    cursor: pointer;
                    margin-bottom: 15px;
                    font-size: 16px;
                ">VERIFY</button>
                <div id="gateway-message" style="color: #ff4444; font-size: 12px; min-height: 40px;"></div>
                <div style="color: #666; font-size: 10px; margin-top: 10px;">
                    ⚠️ ${GATEWAY_CONFIG.MAX_ATTEMPTS} attempts allowed
                </div>
            </div>
        </div>
    `;
    
    document.body.appendChild(modal);
    
    // Focus on input
    const input = document.getElementById('password-input');
    if (input) input.focus();
    
    return modal;
}

function showAccessDenied() {
    document.body.innerHTML = `
        <div style="
            background: #000;
            color: #ff0000;
            font-family: 'Courier New', monospace;
            text-align: center;
            padding: 50px;
            height: 100vh;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
        ">
            <div style="font-size: 48px; margin-bottom: 20px;">⛔</div>
            <div style="font-size: 24px; margin-bottom: 10px;">ACCESS DENIED</div>
            <div style="font-size: 14px; color: #666;">Unauthorized access detected</div>
            <div style="font-size: 12px; margin-top: 30px; color: #444;">
                This incident will be reported
            </div>
        </div>
    `;
}

function updateMessage(text, isError = true) {
    const msgDiv = document.getElementById('gateway-message');
    if (msgDiv) {
        msgDiv.style.color = isError ? '#ff4444' : '#00ff00';
        msgDiv.innerHTML = text;
    }
}

// ========== MAIN AUTHENTICATION FLOW ==========

async function initiateAuthentication() {
    // Check if already authenticated in this session
    if (sessionStorage.getItem(GATEWAY_CONFIG.SESSION_KEY) === 'true') {
        console.log('Already authenticated');
        const terminalContent = document.getElementById('terminal-content');
        if (terminalContent) terminalContent.style.display = 'block';
        return true;
    }
    
    // Check lockout status
    const lockoutCheck = isLockedOut();
    if (lockoutCheck !== false) {
        showAccessDenied();
        console.log(`Locked out for ${lockoutCheck} minutes`);
        return false;
    }
    
    // Create and show modal
    const modal = createPasswordModal();
    
    // Handle verification
    const submitBtn = document.getElementById('submit-password');
    const passwordInput = document.getElementById('password-input');
    
    const attemptVerification = async () => {
        const password = passwordInput.value.trim();
        
        if (!password) {
            updateMessage('Please enter a password');
            return;
        }
        
        // Disable button during verification
        submitBtn.disabled = true;
        submitBtn.style.opacity = '0.5';
        updateMessage('Verifying...', false);
        
        const result = await verifyPassword(password);
        
        if (result.success) {
            // Authentication successful
            updateMessage('✓ ACCESS GRANTED', false);
            resetAttempts();
            sessionStorage.setItem(GATEWAY_CONFIG.SESSION_KEY, 'true');
            
            // Show terminal content
            const terminalContent = document.getElementById('terminal-content');
            if (terminalContent) terminalContent.style.display = 'block';
            
            // Remove modal after slight delay
            setTimeout(() => {
                modal.remove();
            }, 500);
        } else {
            // Authentication failed
            const attempts = incrementAttempts();
            const remaining = GATEWAY_CONFIG.MAX_ATTEMPTS - attempts;
            
            if (attempts >= GATEWAY_CONFIG.MAX_ATTEMPTS) {
                // Max attempts reached - lock out
                const lockoutUntil = Date.now() + GATEWAY_CONFIG.LOCKOUT_TIME;
                sessionStorage.setItem(GATEWAY_CONFIG.LOCKOUT_KEY, lockoutUntil);
                updateMessage(`✗ MAX ATTEMPTS EXCEEDED. LOCKED OUT FOR 15 MINUTES`);
                setTimeout(() => {
                    showAccessDenied();
                }, 1500);
            } else {
                updateMessage(`✗ ${result.message}. ${remaining} attempt${remaining !== 1 ? 's' : ''} remaining`);
                passwordInput.value = '';
                passwordInput.focus();
                submitBtn.disabled = false;
                submitBtn.style.opacity = '1';
            }
        }
    };
    
    submitBtn.addEventListener('click', attemptVerification);
    passwordInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') attemptVerification();
    });
    
    return false; // Not yet authenticated (waiting for user action)
}

// ========== INITIALIZE ON PAGE LOAD ==========

(async function() {
    console.log('🔐 Password Gateway v1.0 - Initializing');
    console.log('🔗 API URL:', GATEWAY_CONFIG.API_URL);
    
    // Hide terminal content initially
    const terminalContent = document.getElementById('terminal-content');
    if (terminalContent) terminalContent.style.display = 'none';
    
    await initiateAuthentication();
})();

// Export for debugging (optional)
window.gatewayConfig = GATEWAY_CONFIG;
