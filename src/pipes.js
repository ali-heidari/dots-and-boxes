import { io } from 'https://cdn.socket.io/4.8.3/socket.io.esm.min.js'

const _rawHub = window.__PIPESHUB_URL__
const HUB_HOSTS   = ['192.168.101.11', '192.168.101.12', '192.168.101.13']
const PIPESHUB_URLS = (_rawHub && _rawHub.startsWith('http')) ? [_rawHub] : HUB_HOSTS.map(h => `http://${h}:3000`)
const AUTH_URLS     = HUB_HOSTS.map(h => `http://${h}:16916/auth`)

const SESSION_KEY = 'dotsboxes.jwt'

let _socket    = null   // Socket.io socket
let _unitName  = null   // registered unit name
let _onMessage = null   // inbound message callback
let _handlers  = {}     // operation name → handler fn (for add())

// ── Public API ───────────────────────────────────────────────────────────────

/**
 * Connect to PipesHub and register as a persistent unit.
 * Retries up to 3 times with 2s backoff before rejecting.
 */
export async function connect(unitName, onMessage) {
  _unitName  = unitName
  _onMessage = onMessage

  const token = await _getToken(unitName)

  let last
  for (const url of PIPESHUB_URLS) {
    try {
      await _openSocket(token, unitName, url)
      return
    } catch (err) {
      last = err
    }
  }
  throw last
}

/**
 * Send a challenge to targetUnitName and await ACCEPT/DECLINE.
 * Rejects with { code: 'TIMEOUT' } after 15s.
 */
export function challenge(targetUnitName, payload) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject({ code: 'TIMEOUT' }), 15_000)

    _socket.emit('gateway', {
      senderId:   _unitName,
      receiverId: targetUnitName,
      operation:  'CHALLENGE',
      input:      payload,
      awaiting:   true,
    })

    _socket.once('responseGateway', data => {
      clearTimeout(timer)
      resolve(data.res ?? data)
    })
  })
}

/** Fire-and-forget move delivery. */
export function sendMove(targetUnitName, move) {
  _safeSend(targetUnitName, 'MOVE', move)
}

/** Fire-and-forget for any message type. */
export function send(targetUnitName, payload) {
  const { type, ...rest } = payload
  _safeSend(targetUnitName, type, rest)
}

/** Disconnect cleanly. */
export function disconnect() {
  if (_socket) { _socket.disconnect(); _socket = null }
}

/** True when the socket exists and is connected. */
export function isConnected() {
  return !!(_socket && _socket.connected)
}

// ── Private ──────────────────────────────────────────────────────────────────

async function _getToken(unitName) {
  const cached = sessionStorage.getItem(SESSION_KEY)
  if (cached !== null) return cached

  let last
  for (const url of AUTH_URLS) {
    try {
      const res = await fetch(url, {
        method:  'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body:    `name=${encodeURIComponent(unitName)}`,
      })
      if (!res.ok) throw new Error(`auth ${res.status}`)
      const token = await res.text()
      sessionStorage.setItem(SESSION_KEY, token)
      return token
    } catch (err) {
      last = err
    }
  }
  throw last
}

function _openSocket(token, unitName, hubUrl) {
  return new Promise((resolve, reject) => {
    _socket = io(hubUrl, {
      query: { name: unitName },
      transportOptions: {
        polling: {
          extraHeaders: { authorization: token },
        },
      },
    })

    _socket.once('connect', () => {
      _socket.on('gateway', _handleGateway)
      _socket.on('disconnect', _handleDisconnect)
      resolve()
    })

    _socket.once('connect_error', err => reject(err))
  })
}

function _handleGateway(data) {
  if (!data || typeof data !== 'object') return
  if (data.receiverId !== _unitName) return

  // Inject pushResponse so inbound ask handlers can reply
  if (data.awaiting) {
    if (!data.input) data.input = {}
    data.input.pushResponse = res => {
      data.res = res
      _socket.emit('responseGateway', data)
    }
  }

  // Route to registered handler or generic onMessage callback
  const handler = _handlers[data.operation]
  if (handler) {
    handler(data.input)
  } else if (_onMessage) {
    _onMessage({ type: data.operation, ...(data.input ?? {}), _raw: data })
  }
}

function _handleDisconnect() {
  document.dispatchEvent(new CustomEvent('pipes-disconnect'))
}

function _safeSend(targetUnitName, operation, input) {
  try {
    _socket.emit('gateway', {
      senderId:   _unitName,
      receiverId: targetUnitName,
      operation,
      input,
      awaiting:   false,
    })
  } catch (err) {
    document.dispatchEvent(new CustomEvent('pipes-error', { detail: err }))
  }
}

function _sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}
