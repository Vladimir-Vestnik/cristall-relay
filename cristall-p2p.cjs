/**
 * 🌐 CRISTALL P2P NETWORK
 * Полноценная децентрализованная сеть на WebSocket
 * 
 * Возможности:
 * - Peer discovery (обнаружение узлов)
 * - Auto-reconnect (автопереподключение)
 * - Direct & Broadcast messaging
 * - Relay routing (для NAT traversal)
 * - DB sync integration
 */

const WebSocket = require('ws');
const EventEmitter = require('events');
const crypto = require('crypto');

/** По адресу (IP или ws://host:port) определить тип соединения: private/LAN => 'local', иначе 'relay'. */
function inferConnectionTypeFromAddress(address) {
  if (!address || typeof address !== 'string') return 'relay';
  let host = address;
  if (host.startsWith('::ffff:')) host = host.slice(7);
  else if (host.startsWith('ws://') || host.startsWith('wss://')) {
    try {
      const u = new URL(host);
      host = u.hostname;
      if (host.startsWith('::ffff:')) host = host.slice(7);
    } catch (_) { return 'relay'; }
  }
  if (host === '127.0.0.1' || host === '::1' || host === 'localhost') return 'local';
  const parts = host.split('.');
  if (parts.length !== 4) return 'relay';
  const a = parseInt(parts[0], 10); const b = parseInt(parts[1], 10);
  if (a === 10) return 'local';
  if (a === 172 && b >= 16 && b <= 31) return 'local';
  if (a === 192 && b === 168) return 'local';
  return 'relay';
}

class CristallP2PNode extends EventEmitter {
  constructor(options = {}) {
    super();
    
    this.nodeId = options.nodeId || this.generateNodeId();
    this.port = options.port || 7777;
    this.isRelayServer = options.isRelayServer || false;
    this.isBridgeMode = options.isBridgeMode || false;
    this.relayAddress = null; // адрес релея для варианта Б (пересылка на релей при отсутствии прямого пира)
    
    // Cristall.ved: тип связи по адресу для исходящих (до handshake_ack)
    this._connectionTypeByAddress = Object.create(null);
    
    // Хранилище подключений
    this.peers = new Map(); // peerId -> { ws, address, metadata, connectionType? }
    this.knownNodes = new Set(); // Список известных адресов
    
    // WebSocket сервер (если мы relay или обычный узел)
    this.server = null;
    
    // WebSocket клиент (подключения к другим узлам)
    this.connections = new Map(); // address -> ws
    this.autoReconnectTargets = new Map(); // address -> bool
    /** Счётчик попыток переподключения по адресу (сбрасывается при успешном подключении). Макс. 3 попытки. */
    this._reconnectAttempts = new Map(); // address -> number
    
    // Настройки
    this.reconnectInterval = options.reconnectInterval || 5000;
    this.maxReconnectAttempts = options.maxReconnectAttempts ?? 3;
    this.heartbeatInterval = options.heartbeatInterval || 30000;
    /** На relay: сколько интервалов без pong считаем peer «зависшим». */
    this.heartbeatMissTolerance = options.heartbeatMissTolerance ?? 2;
    this._heartbeatTimer = null;

    // Метрики трафика
    this.trafficMetrics = {
      bytesSent: 0,
      bytesReceived: 0,
      messagesSent: 0,
      messagesReceived: 0,
      errors: 0,
      lastError: null,
    };
    
    console.log(`[P2P] 🚀 Узел запущен: ${this.nodeId}`);
  }
  
  /**
   * Генерация уникального ID узла
   */
  generateNodeId() {
    return crypto.randomBytes(8).toString('hex');
  }
  
  /**
   * Запуск узла
   */
  async start() {
    // ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Если сервер уже существует, сначала останавливаем его
    if (this.server) {
      console.log(`[P2P] ⚠️ Обнаружен существующий сервер, останавливаем перед запуском нового`);
      try {
        await this.stop();
      } catch (error) {
        console.error(`[P2P] ⚠️ Ошибка остановки существующего сервера:`, error.message);
      }
    }
    
    // ✅ ИСПРАВЛЕНИЕ: Добавляем обработку ошибок при создании сервера
    return new Promise((resolve, reject) => {
      try {
        // Создаём WebSocket сервер
        this.server = new WebSocket.Server({ port: this.port });
        
        // ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Обработка ошибок сервера (предотвращает системное окно Windows)
        this.server.on('error', (error) => {
          console.error(`[P2P] ❌ Ошибка WebSocket сервера на порту ${this.port}:`, error.message);
          if (error.code === 'EADDRINUSE') {
            console.error(`[P2P] ❌ Порт ${this.port} уже занят! Закройте другое приложение, использующее этот порт.`);
          }
          // ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Очищаем ссылку на сервер при ошибке
          this.server = null;
          this.recordError('server_start', error);
          reject(error);
        });
        
        this.server.on('listening', () => {
          console.log(`[P2P] 🌐 Сервер запущен на порту ${this.port}`);
          
          // Обработка входящих подключений
          this.server.on('connection', (ws, req) => {
            const address = req.socket.remoteAddress;
            console.log(`[P2P] ✅ Входящее подключение от ${address}`);
            if (this.isRelayServer) {
              console.log(`[P2P] [RELAY] Входящее подключение: IP клиента (req.socket.remoteAddress)=${address} — этот адрес будет передан в handshake_ack как yourRemoteAddress`);
            }
            
            this.handleIncomingConnection(ws, address, req);
          });
          
          // Запуск heartbeat (проверка живости соединений)
          this.startHeartbeat();
          
          this.emit('started', { nodeId: this.nodeId, port: this.port });
          resolve();
        });
      } catch (error) {
        console.error(`[P2P] ❌ Критическая ошибка при создании сервера:`, error.message);
        this.recordError('server_create', error);
        reject(error);
      }
    });
  }
  
  /**
   * Обработка входящего подключения
   * @param {object} req - запрос сервера (req.socket.remoteAddress, req.socket.remotePort) для сборки адреса пира
   */
  handleIncomingConnection(ws, address, req) {
    let peerId = null;

    ws.removeAllListeners('message');
    ws.removeAllListeners('close');
    ws.removeAllListeners('error');

    ws.on('message', (data) => {
      this.recordIncoming(data);

      let message;
      try {
        message = JSON.parse(data.toString());
      } catch (err) {
        console.error('[P2P] ❌ Ошибка чтения JSON:', err.message);
        return;
      }

      // ⏳ Пока handshake не завершён – не принимаем другие сообщения
      if (!peerId && message.type !== 'handshake') {
        console.warn('[P2P] ⏳ Пропуск сообщения до handshake от', address);
        return;
      }

      // 🤝 Handshake: регистрируем или заменяем peer
      if (message.type === 'handshake') {
        peerId = message.nodeId;
        // Единый формат адреса для отображения: ws://host:port (для входящих берём порт из metadata, иначе только IP)
        let peerAddress = address;
        if (address && (address.startsWith('::ffff:') || !address.includes('://'))) {
          const host = address.startsWith('::ffff:') ? address.slice(7) : address;
          const port = message.metadata && typeof message.metadata.port === 'number' ? message.metadata.port : (req && req.socket && req.socket.remotePort);
          if (port) {
            peerAddress = `ws://${host}:${port}`;
          } else {
            peerAddress = `ws://${host}`;
          }
        }

        const saved = this.peers.get(peerId);

        if (saved && saved.ws === ws) {
          const yourRemoteAddress = address || null;
          const ack = JSON.stringify({
            type: 'handshake_ack',
            nodeId: this.nodeId,
            metadata: this.getNodeMetadata(),
            yourRemoteAddress
          });
          ws.send(ack);
          this.recordOutgoing(ack);
          console.log('[P2P] ✅ Повторный handshake на том же сокете:', peerId);
          return;
        }

        if (saved && saved.ws !== ws) {
          if (saved.ws.readyState === WebSocket.OPEN) {
            if (this.isRelayServer) {
              console.log('[P2P] [RELAY] 🔁 Замена прежнего WebSocket для', peerId, 'новым handshake (reconnect)');
              try { saved.ws.close(1000, 'Superseded by reconnect'); } catch {}
              try { saved.ws.terminate(); } catch {}
            } else {
              console.log('[P2P] ⚠️ WebSocket для', peerId, 'уже активен, закрываем дубликат');
              try { ws.close(1000, 'Duplicate connection'); } catch {}
              return;
            }
          } else {
            console.log('[P2P] 🔁 Старый WebSocket для', peerId, 'не активен, обновляем');
            try { saved.ws.terminate(); } catch {}
          }
        }

        const connectionType = inferConnectionTypeFromAddress(peerAddress);
        this.peers.set(peerId, {
          ws,
          address: peerAddress,
          metadata: message.metadata || {},
          connectionType,
          lastPongAt: Date.now()
        });

        const yourRemoteAddress = address || null;
        if (this.isRelayServer && yourRemoteAddress) {
          console.log(`[P2P] [RELAY] Handshake от nodeId=${peerId}, IP клиента (remoteAddress)=${yourRemoteAddress} → в handshake_ack yourRemoteAddress=${yourRemoteAddress}`);
        }
        const ack = JSON.stringify({
          type: 'handshake_ack',
          nodeId: this.nodeId,
          metadata: this.getNodeMetadata(),
          yourRemoteAddress
        });
        ws.send(ack);
        this.recordOutgoing(ack);

        console.log('[P2P] ✅ Handshake выполнен с', peerId, ', address=', peerAddress);
        this.emit('peer:connected', { peerId, address: peerAddress });
        return;
      }

      // 📩 Обрабатываем остальные сообщения ТОЛЬКО после handshake
      this.handleMessage(message, peerId);
    });

    // ❌ При закрытии – удаляем peer только если сокет совпадает с сохранённым
    ws.on('close', (code, reason) => {
      console.log(`[P2P] [RELAY-CLOSE] Соединение закрыто: peerId=${peerId}, code=${code}, reason=${reason?.toString() || 'none'}, address=${address}`);
      const saved = this.peers.get(peerId);
      if (saved?.ws === ws) {
        this.peers.delete(peerId);
        console.log('[P2P] ❌ Peer отключился:', peerId);
        this.emit('peer:disconnected', { peerId });
      } else {
        console.log(`[P2P] [RELAY-CLOSE] ⚠️ WebSocket не совпадает - это старое соединение, игнорируем`);
      }
    });

    ws.on('error', (err) => {
      console.error('[P2P] ⚠️ Ошибка соединения:', err.message);
      this.recordError('incoming_ws', err);
    });
  }
  
  /**
   * Подключение к другому узлу
   */
  async connectToPeer(address, options = {}) {
    if (this.connections.has(address)) {
      console.log(`[P2P] ⚠️ Уже подключены к ${address}`);
      // ✅ ИСПРАВЛЕНИЕ: Если соединение уже есть, проверяем, есть ли peer, и отправляем событие
      const existingConnection = this.connections.get(address);
      if (existingConnection && existingConnection.readyState === WebSocket.OPEN) {
        // Ищем peer по адресу
        let foundPeer = null;
        for (const [peerId, peerInfo] of this.peers.entries()) {
          if (peerInfo.address === address || peerInfo.ws === existingConnection) {
            foundPeer = peerId;
            break;
          }
        }
        
        if (foundPeer) {
          console.log(`[P2P] ✅ Соединение уже активно, отправляем событие peer:connected для ${foundPeer}`);
          this.emit('peer:connected', { peerId: foundPeer, address });
        } else {
          // Соединение активно, но peer ещё не добавлен (handshake в процессе)
          // Отправляем событие с временным peerId, чтобы обновить статус
          console.log(`[P2P] ✅ Соединение активно, но peer ещё не определён (handshake в процессе), отправляем событие с временным ID`);
          // Используем адрес как временный peerId
          const tempPeerId = `temp_${address.replace(/[^a-zA-Z0-9]/g, '_')}`;
          this.emit('peer:connected', { peerId: tempPeerId, address });
        }
      } else {
        console.log(`[P2P] ⚠️ Соединение существует, но не активно (readyState: ${existingConnection?.readyState})`);
      }
      return;
    }
    
    console.log(`[P2P] 🔌 Подключаемся к ${address}...`);
    
    if (options.connectionType === 'local' || options.connectionType === 'relay') {
      this._connectionTypeByAddress[address] = options.connectionType;
    }
    const allowReconnect = options.allowReconnect !== false;
    this.autoReconnectTargets.set(address, allowReconnect);
    
    // ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Создаём Promise, который разрешается после установления соединения
    return new Promise((resolve, reject) => {
      let connectionEstablished = false; // Объявляем в начале Promise
      const connectStartTime = Date.now(); // Время начала подключения
      
      const connectionTimeout = setTimeout(() => {
        const elapsed = Date.now() - connectStartTime;
        if (!connectionEstablished || !this.connections.has(address) || this.connections.get(address)?.readyState !== WebSocket.OPEN) {
          console.error(`[P2P] ❌ Таймаут подключения к ${address} (прошло ${elapsed}мс из 10000мс)`);
          console.error(`[P2P] ❌ connectionEstablished=${connectionEstablished}, hasConnection=${this.connections.has(address)}, readyState=${this.connections.get(address)?.readyState}`);
          this.recordError('connection_timeout', new Error('Connection timeout'));
          reject(new Error(`Connection timeout: не удалось подключиться к ${address} за 10 секунд`));
        }
      }, 10000); // 10 секунд таймаут
      
      try {
        console.log(`[P2P] 🔌 Создаём WebSocket для ${address}... (время: ${new Date().toISOString()})`);
        const ws = new WebSocket(address);
        console.log(`[P2P] 🔌 WebSocket создан, readyState=${ws.readyState} (0=CONNECTING)`);
        let messagesReceived = 0; // Счетчик полученных сообщений для диагностики
        
        // ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Устанавливаем обработчик open ПЕРВЫМ, чтобы не пропустить событие
        ws.on('open', () => {
          const connectDuration = Date.now() - connectStartTime;
          console.log(`[P2P] [WS-OPEN] ✅ Подключение установлено: ${address}, readyState=${ws.readyState}, время подключения: ${connectDuration}мс`);
          clearTimeout(connectionTimeout);
          connectionEstablished = true;
          
          // Отправляем handshake
          const payload = JSON.stringify({
            type: 'handshake',
            nodeId: this.nodeId,
            metadata: this.getNodeMetadata()
          });
          ws.send(payload);
          this.recordOutgoing(payload);
          
          this.connections.set(address, ws);
          this._reconnectAttempts.set(address, 0); // успех — сбрасываем счётчик попыток
          
          // Сохраняем время открытия соединения для диагностики
          ws._connectedAt = Date.now();
          
          // ❌ УДАЛЕНО: Мониторинг WebSocket каждые 5 секунд убран для уменьшения логов
          // Мониторинг засорял логи и не приносил пользы в продакшене
          
          // ✅ Разрешаем Promise после установления соединения
          // Не ждём handshake_ack, т.к. он может прийти позже
          resolve();
        });
        
        // ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Устанавливаем обработчик message ДО open, чтобы не потерять сообщения
        ws.on('message', (data) => {
          messagesReceived++;
          const timestamp = Date.now();
          console.log(`[P2P] [INCOMING-RAW] [${timestamp}] Message #${messagesReceived} from ${address}, length=${data.length}, readyState=${ws.readyState}, isBuffer=${Buffer.isBuffer(data)}`);
          this.recordIncoming(data);
          try {
            const rawText = data.toString();
            console.log(`[P2P] [INCOMING-RAW] Raw text: ${rawText.substring(0, 200)}${rawText.length > 200 ? '...' : ''}`);
            const message = JSON.parse(rawText);
            console.log(`[P2P] [INCOMING-WS] Message #${messagesReceived} from ${address}, type=${message.type}, from=${message.from || 'none'}`);
            
            if (message.type === 'handshake_ack') {
              const peerId = message.nodeId;
              const connectionType = (this._connectionTypeByAddress && this._connectionTypeByAddress[address]) || 'relay';
              if (this._connectionTypeByAddress && this._connectionTypeByAddress[address]) {
                delete this._connectionTypeByAddress[address];
              }
              this.peers.set(peerId, { ws, address, metadata: message.metadata, connectionType });
              // Адрес, как нас видит сервер (relay) — для Cristall.ved объявляем его в presence
              if (message.yourRemoteAddress != null) {
                this._seenAddressByServer = this._seenAddressByServer || Object.create(null);
                this._seenAddressByServer[address] = message.yourRemoteAddress;
                console.log(`[P2P] [CLIENT] handshake_ack: наш адрес от relay (yourRemoteAddress)=${message.yourRemoteAddress}, сохраняем под ключом ws=${address}`);
              } else {
                console.log(`[P2P] [CLIENT] handshake_ack: relay не передал yourRemoteAddress (будет fallback 127.0.0.1)`);
              }
              console.log(`[P2P] 🤝 Handshake ACK от ${peerId} (сообщение #${messagesReceived})`);
              console.log(`[P2P] 📤 Отправка события peer:connected для ${peerId} (address: ${address})`);
              this.emit('peer:connected', { peerId, address, connectionType });
              console.log(`[P2P] ✅ Событие peer:connected отправлено для ${peerId}`);
              console.log(`[P2P] [WS-STATE-AFTER-HANDSHAKE] WebSocket readyState=${ws.readyState}, address=${address}, listeners count: message=${ws.listenerCount('message')}, close=${ws.listenerCount('close')}, error=${ws.listenerCount('error')}, messagesReceived=${messagesReceived}`);
              
              // ✅ ДИАГНОСТИКА: Проверяем, что WebSocket объект в peers совпадает с текущим
              const savedPeer = this.peers.get(peerId);
              if (savedPeer && savedPeer.ws !== ws) {
                console.error(`[P2P] ❌ [WS-MISMATCH] WebSocket объект в peers НЕ совпадает с текущим! Это может быть причиной потери сообщений.`);
              } else {
                console.log(`[P2P] ✅ [WS-MATCH] WebSocket объект в peers совпадает с текущим`);
              }
            } else {
              console.log(`[P2P] [INCOMING-WS] Message #${messagesReceived} calling handleMessage for type=${message.type}, from=${message.from || 'unknown'}`);
              
              // ✅ ДИАГНОСТИКА: Проверяем, что WebSocket объект в peers совпадает с текущим
              if (message.from) {
                const savedPeer = this.peers.get(message.from);
                if (savedPeer && savedPeer.ws !== ws) {
                  console.error(`[P2P] ❌ [WS-MISMATCH] WebSocket объект в peers (${message.from}) НЕ совпадает с текущим! Это может быть причиной потери сообщений.`);
                }
              }
              
              this.handleMessage(message, message.from);
            }
          } catch (error) {
            console.error(`[P2P] ❌ Ошибка обработки сообщения:`, error.message);
            console.error(`[P2P] ❌ Stack:`, error.stack);
            console.error(`[P2P] ❌ Raw data:`, data.toString().substring(0, 500));
          }
        });
        
        ws.on('close', (code, reason) => {
          const timeSinceOpen = ws._connectedAt ? Date.now() - ws._connectedAt : 'unknown';
          const timeSinceOpenSec = typeof timeSinceOpen === 'number' ? (timeSinceOpen / 1000).toFixed(1) : 'unknown';
          
          console.log(`[P2P] [WS-CLOSE] Соединение закрыто: ${address}, code=${code}, reason=${reason?.toString() || 'none'}`);
          console.log(`[P2P] [WS-CLOSE-DEBUG] Время жизни соединения: ${timeSinceOpenSec} сек (${timeSinceOpen}ms)`);
          console.log(`[P2P] [WS-CLOSE-DEBUG] WebSocket состояние: readyState=${ws.readyState}, bufferedAmount=${ws.bufferedAmount}`);
          
          // Анализ кода закрытия для диагностики провайдера
          if (code === 1006) {
            console.log(`[P2P] [WS-CLOSE-DEBUG] ⚠️ Code 1006 - аномальное закрытие (без close frame)`);
            if (typeof timeSinceOpen === 'number' && timeSinceOpen > 0) {
              console.log(`[P2P] [WS-CLOSE-DEBUG] 💡 Если закрытие происходит регулярно через ~${Math.round(timeSinceOpen / 1000)} сек - возможно таймаут провайдера/NAT`);
            }
            console.log(`[P2P] [WS-CLOSE-DEBUG] Возможные причины: провайдер/NAT закрыл соединение, сетевой разрыв, или сервер закрыл без close frame`);
          } else if (code === 1000) {
            console.log(`[P2P] [WS-CLOSE-DEBUG] ✅ Code 1000 - нормальное закрытие`);
          } else if (code === 1001) {
            console.log(`[P2P] [WS-CLOSE-DEBUG] Code 1001 - сервер уходит (going away)`);
          } else if (code === 1002) {
            console.log(`[P2P] [WS-CLOSE-DEBUG] Code 1002 - ошибка протокола`);
          } else {
            console.log(`[P2P] [WS-CLOSE-DEBUG] Code ${code} - неизвестный код закрытия`);
          }
          
          this.connections.delete(address);
          
          // ✅ Удаляем peer из peers и эмитим peer:disconnected (иначе UI показывает "подключено" при уже закрытом сокете)
          let peerIdToEmit = null;
          for (const [pid, p] of this.peers.entries()) {
            if (p.ws === ws || p.address === address) {
              peerIdToEmit = pid;
              this.peers.delete(pid);
              console.log(`[P2P] ❌ [OUTGOING-CLOSE] Peer удалён из списка: ${pid}, address=${address}`);
              break;
            }
          }
          if (peerIdToEmit) {
            this.emit('peer:disconnected', { peerId: peerIdToEmit });
          }
          
          const shouldReconnect = this.autoReconnectTargets.get(address);
          if (shouldReconnect) {
            const attempt = (this._reconnectAttempts.get(address) || 0) + 1;
            this._reconnectAttempts.set(address, attempt);
            const maxAttempts = this.maxReconnectAttempts;
            if (attempt <= maxAttempts) {
              const delay = this.reconnectInterval * Math.min(attempt, 3); // 5s, 10s, 15s при 5s base
              console.log(`[P2P] 🔄 Переподключение к ${address}: попытка ${attempt}/${maxAttempts} через ${delay / 1000}с`);
              setTimeout(() => {
                if (this.autoReconnectTargets.get(address) && !this.connections.has(address)) {
                  this.connectToPeer(address, { allowReconnect: true });
                } else {
                  console.log(`[P2P] ⏹️ Автопереподключение отменено для ${address}`);
                }
              }, delay);
            } else {
              console.log(`[P2P] ⏹️ Переподключение к ${address} остановлено после ${maxAttempts} попыток`);
              this._reconnectAttempts.set(address, 0); // сброс для следующей серии
            }
          } else {
            console.log(`[P2P] 🔕 Автопереподключение отключено для ${address}`);
          }
        });
        
        ws.on('error', (error) => {
          console.error(`[P2P] [WS-ERROR] ❌ Ошибка WebSocket ${address}:`, error.message);
          console.error(`[P2P] [WS-ERROR] ❌ readyState=${ws.readyState}, code=${error.code || 'N/A'}`);
          console.error(`[P2P] [WS-ERROR] ❌ Stack:`, error.stack);
          this.recordError('outgoing_ws', error);
          clearTimeout(connectionTimeout);
          
          // ✅ Отклоняем Promise при ошибке подключения
          if (!connectionEstablished) {
            reject(error);
          }
        });
        
      } catch (error) {
        console.error(`[P2P] ❌ Не удалось подключиться к ${address}:`, error.message);
        this.recordError('connect_failure', error);
        clearTimeout(connectionTimeout);
        reject(error);
      }
    });
  }
  
  /**
   * Обработка входящих сообщений
   */
  handleMessage(message, fromPeerId) {
    console.log(`[P2P] [MESSAGE-RECEIVED] From ${fromPeerId}: type=${message.type}`);
    if (message.type === 'db_ad_image_response') {
      const base64Len = typeof message.base64 === 'string' ? message.base64.length : 0;
      console.log(`[P2P] [MESSAGE-FULL] type=${message.type}, filename=${message.filename}, base64 length=${base64Len}`);
    } else {
      console.log(`[P2P] [MESSAGE-FULL]`, JSON.stringify(message, null, 2));
    }
    
    // ✅ ИСПРАВЛЕНИЕ: Если сообщение пришло через relay с полем 'to', используем message.from как реального отправителя
    // Это нужно, потому что fromPeerId в этом случае будет ID relay, а не реального отправителя
    if (!this.isRelayServer && message.to && message.to === this.nodeId && message.from) {
      console.log(`[P2P] 📨 Сообщение через relay: fromPeerId=${fromPeerId} (relay), реальный отправитель=${message.from}, type=${message.type}`);
      fromPeerId = message.from; // Используем реального отправителя вместо ID relay
    }
    
    // Если сообщение для relay (пересылка)
    if (this.isRelayServer && message.to && message.to !== this.nodeId) {
      console.log(`[P2P] 🔄 Relay: пересылка сообщения ${message.type} от ${message.from || fromPeerId} к ${message.to}`);
      this.relayMessage(message, fromPeerId);
      return;
    }
    
    // Режим моста (mesh): пересылка сообщения конечному получателю или на релей
    const noForwardTypes = ['ping', 'pong', 'handshake', 'handshake_ack', 'peer_discovery', 'peer_list', 'metrics_ping', 'metrics_pong'];
    const isBridgeForwardable = (t) => {
      if (!t || noForwardTypes.includes(t) || t.startsWith('db_')) return false;
      return t.startsWith('messenger_') || t.startsWith('call_') || t === 'webrtc_message' ||
        ['host_changed', 'media_control', 'permission_changed', 'hand_raised', 'participant_removed'].includes(t) ||
        t.startsWith('screen_sharing_');
    };
    
    if (!this.isRelayServer && this.isBridgeMode && message.to && message.to !== this.nodeId) {
      if (!isBridgeForwardable(message.type)) {
        console.log(`[P2P] ⏭️ Мост: тип ${message.type} не пересылаем`);
        return;
      }
      if (message.from === this.nodeId) {
        console.log(`[P2P] ⏭️ Мост: не пересылаем свои сообщения (защита от петель)`);
        return;
      }
      const target = this.peers.get(message.to);
      if (target && target.ws.readyState === WebSocket.OPEN) {
        console.log(`[P2P] 🌉 Мост: пересылка ${message.type} пиру ${message.to}`);
        this.forwardMessage(message);
        return;
      }
      // Вариант Б: пира нет — пересылаем на релей (если есть)
      const relayPeerId = this._findRelayPeer();
      if (relayPeerId) {
        console.log(`[P2P] 🌉 Мост: пира ${message.to} нет, пересылаем на релей ${relayPeerId}`);
        this.sendRawToPeer(relayPeerId, message);
        return;
      }
      console.log(`[P2P] ⏭️ Мост: пира ${message.to} нет, релея нет — игнорируем`);
      return;
    }
    
    // Если это не relay и сообщение не для нас, игнорируем его
    if (!this.isRelayServer && message.to && message.to !== this.nodeId) {
      console.log(`[P2P] ⏭️ Игнорируем сообщение ${message.type}: оно не для нас (to=${message.to}, мы=${this.nodeId})`);
      return;
    }
    
    // Если это relay и сообщение без поля to, но это не системное сообщение
    if (this.isRelayServer && !message.to && message.type && !message.type.startsWith('db_') && message.type !== 'ping' && message.type !== 'pong' && message.type !== 'peer_discovery' && message.type !== 'peer_list' && message.type !== 'metrics_ping' && message.type !== 'metrics_pong') {
      console.warn(`[P2P] ⚠️ Relay получил сообщение ${message.type} без поля 'to' от ${fromPeerId}. Сообщение не может быть переслано.`);
    }
    
    // Если это relay сервер и сообщение БД - пересылаем всем остальным узлам
    if (this.isRelayServer && message.type && message.type.startsWith('db_')) {
      console.log(`[P2P] 🔄 Relay: пересылка ${message.type} от ${fromPeerId} всем узлам`);
      this.relayBroadcast(message, fromPeerId);
    }

    // Cristall.ved: relay пересылает протокольные сообщения всем (presence, sync_*, user_*)
    const cristallVedTypes = ['presence', 'sync_request', 'sync_claim', 'sync_done', 'user_lookup', 'user_reply'];
    if (this.isRelayServer && message.type && cristallVedTypes.includes(message.type)) {
      console.log(`[P2P] 🔄 Relay: пересылка Cristall.ved ${message.type} от ${fromPeerId} всем узлам`);
      this.relayBroadcast(message, fromPeerId);
    }
    
    // Обработка различных типов сообщений
    switch (message.type) {
      case 'ping':
        this.sendToPeer(fromPeerId, { type: 'pong', timestamp: Date.now() });
        break;

      case 'pong':
        if (fromPeerId) {
          const peer = this.peers.get(fromPeerId);
          if (peer) peer.lastPongAt = Date.now();
        }
        break;
        
      case 'peer_discovery':
        // Отправляем список известных узлов
        try {
        console.log(`[P2P] 📋 Обработка peer_discovery от ${fromPeerId}, this.peers.size = ${this.peers.size}`);
        const knownPeers = this.getKnownPeers();
        console.log(`[P2P] 📋 Обработка peer_discovery от ${fromPeerId}, отправляем ${knownPeers.length} узлов`);
          if (Array.isArray(knownPeers) && knownPeers.length > 0) {
            console.log(`[P2P] 📋 Структура knownPeers:`, knownPeers.map(p => ({ peerId: p.peerId, address: p.address })));
          } else {
            console.log(`[P2P] 📋 knownPeers пустой или не массив`);
          }
          const peerListMessage = {
            type: 'peer_list',
            peers: knownPeers,
            from: this.nodeId,
            to: fromPeerId,
            timestamp: Date.now()
          };
          console.log(`[P2P] 📋 Отправляем peer_list:`, {
            type: peerListMessage.type,
            peersCount: peerListMessage.peers.length,
            from: peerListMessage.from,
            to: peerListMessage.to
          });
          const sent = this.sendToPeer(fromPeerId, peerListMessage);
          if (sent) {
            console.log(`[P2P] ✅ peer_list отправлен узлу ${fromPeerId}`);
          } else {
            console.error(`[P2P] ❌ Не удалось отправить peer_list узлу ${fromPeerId}`);
          }
        } catch (error) {
          console.error(`[P2P] ❌ Ошибка обработки peer_discovery:`, error.message);
          console.error(`[P2P] ❌ Stack:`, error.stack);
        }
        break;
      
      case 'metrics_ping':
        console.log(`[P2P] [METRICS-PING] Received metrics_ping from ${fromPeerId}, pingId=${message.pingId}`);
        const pongMessage = {
          type: 'metrics_pong',
          pingId: message.pingId,
          sentAt: message.sentAt,
          repliedAt: Date.now()
        };
        const pongSent = this.sendToPeer(fromPeerId, pongMessage);
        if (pongSent) {
          console.log(`[P2P] [METRICS-PONG] metrics_pong sent to ${fromPeerId}`);
        } else {
          console.error(`[P2P] [METRICS-PONG] Failed to send metrics_pong to ${fromPeerId}`);
        }
        break;

      case 'metrics_pong':
        console.log(`[P2P] [METRICS-PONG] Received metrics_pong from ${fromPeerId}, pingId=${message.pingId}`);
        this.emit('metrics:pong', {
          from: fromPeerId,
          pingId: message.pingId,
          sentAt: message.sentAt,
          repliedAt: message.repliedAt,
          receivedAt: Date.now()
        });
        break;
        
      case 'db_ping':
      case 'db_pong':
      case 'db_sync_request':
      case 'db_sync_response':
      case 'db_update':
      case 'db_full_sync':
      case 'db_proposal_created':
      case 'db_proposal_updated':
      case 'db_proposal_deleted':
        // Передаём в систему синхронизации БД
        this.emit('db:message', { from: fromPeerId, message });
        break;
        
      case 'db_account_updated':
        // ✅ Передаём обновление account в систему синхронизации БД
        this.emit('db:message', { from: fromPeerId, message });
        break;
        
      case 'db_comment_created':
      case 'db_comment_updated':
      case 'db_comment_deleted':
        // ✅ Передаём события комментариев в систему синхронизации БД
        this.emit('db:message', { from: fromPeerId, message });
        break;
        
      case 'db_ad_created':
      case 'db_ad_updated':
      case 'db_ad_deleted':
      case 'db_ad_comment_created':
      case 'db_ad_comment_updated':
      case 'db_ad_comment_deleted':
      case 'db_favorite_added':
      case 'db_favorite_removed':
      case 'db_ad_image_request':
      case 'db_ad_image_response':
      case 'db_ad_image_not_found':
      case 'db_avatar_image_request':
      case 'db_avatar_image_response':
      case 'db_avatar_image_not_found':
        // ✅ Передаём события resursblock и аватарок в систему синхронизации БД
        this.emit('db:message', { from: fromPeerId, message });
        break;

      case 'presence':
        // Cristall.ved: для bootstrap — передаём в connect-via-cristall-ved (исключение себя по nodeId там)
        if (!this.isRelayServer) {
          console.log(`[P2P] [CLIENT] Получен presence: nodeId=${message.nodeId}, address=${message.address || '(нет)'}, addressLocal=${message.addressLocal || '(нет)'}`);
        }
        this.emit('cristall_ved:presence', {
          nodeId: message.nodeId,
          address: message.address,
          addressRelay: message.address || null,
          addressLocal: message.addressLocal || null,
          userHash: message.userHash || null
        });
        break;

      case 'mesh_bridge_candidate':
        // Мэш: доставка подписчикам (выбор моста по метрикам relay)
        this.emit('message', { from: fromPeerId, message });
        break;
        
      default:
        // Пользовательские сообщения (включая peer_list, messenger_message и т.д.)
        console.log(`[P2P] [EMIT-MESSAGE] Emitting 'message' event for type ${message.type} from ${fromPeerId}`);
        this.emit('message', { from: fromPeerId, message });
    }
  }
  
  /**
   * Relay - пересылка сообщения другому узлу
   */
  relayMessage(message, fromPeerId) {
    console.log('[P2P] 🔄 Relay-пересылка:', message.type, 'к', message.to);

    const target = this.peers.get(message.to);
    if (target && target.ws.readyState === WebSocket.OPEN) {
      const payload = JSON.stringify(message);
      try {
        target.ws.send(payload);
        this.recordOutgoing(payload);
        console.log('[P2P] ✅ Relay: сообщение отправлено узлу', message.to);
        return true;
      } catch (err) {
        console.error('[P2P] ❌ Relay send error:', err.message);
        this.recordError('relay_send', err);
        return false;
      }
    }
    // Получателя нет в peers — рассылаем всем (кроме отправителя) для доставки через мост
    const excludeFrom = fromPeerId != null ? fromPeerId : message.from;
    console.log('[P2P] 🔄 Relay: получатель', message.to, 'не в peers, broadcast всем кроме', excludeFrom);
    return this.relayBroadcast(message, excludeFrom) > 0;
  }

  /**
   * Пересылка сообщения пиру message.to без изменения from/to (режим моста)
   */
  forwardMessage(message) {
    const target = this.peers.get(message.to);
    if (!target || target.ws.readyState !== WebSocket.OPEN) return false;
    try {
      const payload = JSON.stringify(message);
      target.ws.send(payload);
      this.recordOutgoing(payload);
      return true;
    } catch (err) {
      console.error('[P2P] ❌ forwardMessage error:', err.message);
      return false;
    }
  }

  /**
   * Отправка сообщения пиру «как есть» (без подмены from) — для пересылки мостом на релей
   */
  sendRawToPeer(peerId, message) {
    const peer = this.peers.get(peerId);
    if (!peer || peer.ws.readyState !== WebSocket.OPEN) return false;
    try {
      const payload = JSON.stringify(message);
      peer.ws.send(payload);
      this.recordOutgoing(payload);
      return true;
    } catch (err) {
      console.error('[P2P] ❌ sendRawToPeer error:', err.message);
      return false;
    }
  }

  _findRelayPeer() {
    if (!this.relayAddress) return null;
    for (const [peerId, peer] of this.peers.entries()) {
      const addr = peer.address || '';
      if (addr === this.relayAddress) return peerId;
      const relayHost = this.relayAddress.replace(/^wss?:\/\//, '').split('/')[0];
      if (relayHost && (addr.includes(relayHost) || addr === relayHost)) return peerId;
    }
    return null;
  }
  
  /**
   * Relay Broadcast - пересылка сообщения всем узлам кроме отправителя
   */
  relayBroadcast(message, fromPeerId) {
    const messageStr = JSON.stringify(message);
    let sent = 0;
    
    this.peers.forEach((peer, peerId) => {
      // Не отправляем обратно отправителю
      if (peerId !== fromPeerId && peer.ws.readyState === WebSocket.OPEN) {
        peer.ws.send(messageStr);
        this.recordOutgoing(messageStr);
        sent++;
      }
    });
    
    console.log(`[P2P] 🔄 Relay broadcast: переслано ${sent} узлам (кроме ${fromPeerId})`);
    return sent;
  }
  
  /**
   * Отправка сообщения конкретному узлу
   */
  sendToPeer(peerId, message) {
    const sendId = `[SEND-TO-PEER-${Date.now()}-${Math.random().toString(36).substr(2, 9)}]`;
    console.log(`[P2P] 🚀 ${sendId} sendToPeer вызван: peerId=${peerId}, type=${message.type}, global_id=${message.data?.global_message_id}`);
    console.log(`[P2P] ${sendId} Stack:`, new Error().stack.split('\n').slice(1, 5).join(' -> '));
    
    // Детальное логирование для диагностики
    console.log(`[P2P] 🔍 ${sendId} Ищем узел: ${peerId}`);
    console.log(`[P2P] 🔍 ${sendId} Всего подключено peers: ${this.peers.size}`);
    if (this.peers.size > 0) {
      const allPeerIds = Array.from(this.peers.keys());
      console.log(`[P2P] 🔍 ${sendId} Доступные peer IDs:`, allPeerIds);
      console.log(`[P2P] 🔍 ${sendId} Ищем точное совпадение для: ${peerId}`);
      const exactMatch = allPeerIds.find(id => id === peerId);
      console.log(`[P2P] 🔍 ${sendId} Точное совпадение:`, exactMatch ? '✅ НАЙДЕНО' : '❌ НЕ НАЙДЕНО');
    }
    
    let peer = this.peers.get(peerId);
    let sendViaRelay = false;
    if (!peer && this.peers.size > 0) {
      const firstPeerId = this.peers.keys().next().value;
      peer = this.peers.get(firstPeerId);
      if (peer && peer.ws.readyState === WebSocket.OPEN) {
        sendViaRelay = true;
        console.log(`[P2P] 📤 ${sendId} Отправка через relay (to=${peerId})`);
      }
    }
    if (!peer) {
      console.error(`[P2P] ❌ ${sendId} Узел ${peerId} не найден`);
      this.recordError('send_peer_missing', new Error(`peer ${peerId} not found`));
      return false;
    }
    if (peer.ws.readyState !== WebSocket.OPEN) {
      console.error(`[P2P] ❌ ${sendId} Соединение с ${peerId} не активно`);
      this.recordError('send_peer_closed', new Error(`peer ${peerId} socket not open`));
      return false;
    }
    const envelope = {
      ...message,
      from: this.nodeId,
      timestamp: Date.now()
    };
    if (sendViaRelay) envelope.to = peerId;
    const payload = JSON.stringify(envelope);
    if (!payload || payload === '{}' || payload === 'null') {
      console.error('[P2P] ❌ SEND-BLOCK: Пустой или неопределённый payload, не отправляем.');
      return false;
    }
    try {
      peer.ws.send(payload);
      this.recordOutgoing(payload);
      return true;
    } catch (error) {
      console.error(`[P2P] ❌ ${sendId} Ошибка отправки:`, error.message);
      this.recordError('send_error', error);
      return false;
    }
  }
  
  /**
   * Broadcast - отправка сообщения ВСЕМ узлам
   */
  broadcast(message) {
    const envelope = {
      ...message,
      from: this.nodeId,
      timestamp: Date.now()
    };
    
    const messageStr = JSON.stringify(envelope);
    let sent = 0;
    
    this.peers.forEach((peer, peerId) => {
      if (peer.ws.readyState === WebSocket.OPEN) {
        peer.ws.send(messageStr);
        this.recordOutgoing(messageStr);
        sent++;
      }
    });
    
    console.log(`[P2P] 📤 Broadcast отправлен ${sent} узлам`);
    return sent;
  }
  
  /**
   * Получение метаданных узла
   */
  getNodeMetadata() {
    return {
      version: '1.0.0',
      capabilities: ['db_sync', 'webrtc', 'relay'],
      timestamp: Date.now(),
      port: this.port
    };
  }
  
  /**
   * Получение списка известных узлов
   */
  getKnownPeers() {
    const peers = [];
    console.log(`[P2P] 🔍 getKnownPeers: this.peers.size = ${this.peers.size}`);
    this.peers.forEach((peer, peerId) => {
      console.log(`[P2P] 🔍 getKnownPeers: добавляем узел ${peerId}, address=${peer.address}, ws.readyState=${peer.ws?.readyState}`);
      peers.push({
        peerId,
        address: peer.address,
        metadata: peer.metadata
      });
    });
    console.log(`[P2P] 🔍 getKnownPeers: возвращаем ${peers.length} узлов`);
    return peers;
  }
  
  /**
   * Heartbeat - проверка живости соединений
   */
  startHeartbeat() {
    if (this._heartbeatTimer) return;
    const staleMs = this.heartbeatInterval * this.heartbeatMissTolerance;
    this._heartbeatTimer = setInterval(() => {
      const now = Date.now();
      for (const [peerId, peer] of [...this.peers.entries()]) {
        if (!peer.ws || peer.ws.readyState !== WebSocket.OPEN) continue;

        const lastPongAt = peer.lastPongAt || 0;
        if (this.isRelayServer && lastPongAt > 0 && now - lastPongAt > staleMs) {
          console.log(`[P2P] [RELAY] Heartbeat timeout для ${peerId}, удаляем зависший peer (${Math.round((now - lastPongAt) / 1000)}с без pong)`);
          try { peer.ws.terminate(); } catch {}
          this.peers.delete(peerId);
          this.emit('peer:disconnected', { peerId });
          continue;
        }

        this.sendToPeer(peerId, { type: 'ping', timestamp: now });
      }
    }, this.heartbeatInterval);
  }
  
  /**
   * Отключиться от одного адреса (например от bootstrap relay), не останавливая узел.
   * Для режима Cristall.ved: после отправки presence отключиться от relay.
   */
  disconnectFromAddress(address) {
    if (!address) return;
    this.autoReconnectTargets.delete(address);
    let peerIdToRemove = null;
    for (const [pid, peer] of this.peers.entries()) {
      if (peer.address === address) {
        peerIdToRemove = pid;
        break;
      }
    }
    if (peerIdToRemove !== null) {
      const peer = this.peers.get(peerIdToRemove);
      if (peer && peer.ws) {
        try {
          peer.ws.close(1000, 'Cristall.ved: disconnect from bootstrap');
        } catch (e) {
          try { peer.ws.terminate(); } catch (_) {}
        }
      }
      this.peers.delete(peerIdToRemove);
      this.emit('peer:disconnected', { peerId: peerIdToRemove });
    }
    const ws = this.connections.get(address);
    if (ws) {
      try {
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) ws.close(1000, 'Cristall.ved: disconnect from bootstrap');
      } catch (e) {
        try { ws.terminate(); } catch (_) {}
      }
      this.connections.delete(address);
    }
  }

  /**
   * Остановка узла
   */
  async stop() {
    console.log('[P2P] 🛑 Остановка узла...');

    if (this._heartbeatTimer) {
      clearInterval(this._heartbeatTimer);
      this._heartbeatTimer = null;
    }
    
    // ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Останавливаем автопереподключение ДО закрытия соединений
    this.autoReconnectTargets.clear();
    
    // ✅ ИСПРАВЛЕНИЕ: Очистка всех зависших сокетов через terminate
    // Это гарантирует, что relay не держит призраков-соединений
    for (const peer of this.peers.values()) {
      try {
        if (peer.ws) {
          peer.ws.terminate(); // terminate() принудительно закрывает соединение
        }
      } catch (error) {
        // Игнорируем ошибки при закрытии
      }
    }
    this.peers.clear();
    
    // ✅ ИСПРАВЛЕНИЕ: Очистка всех исходящих соединений
    for (const ws of this.connections.values()) {
      try {
        if (ws) {
          ws.terminate(); // terminate() принудительно закрывает соединение
        }
      } catch (error) {
        // Игнорируем ошибки при закрытии
      }
    }
    this.connections.clear();
    
    // ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Корректно закрываем сервер с таймаутом
    if (this.server) {
      return new Promise((resolve) => {
        // Таймаут на закрытие сервера (5 секунд)
        const timeout = setTimeout(() => {
          console.error('[P2P] ⚠️ Таймаут закрытия сервера, принудительно освобождаем порт');
          if (this.server) {
            try {
              // Принудительно закрываем все соединения сервера
              this.server.clients.forEach((client) => {
                try {
                  client.terminate();
                } catch (e) {
                  console.error('[P2P] ⚠️ Ошибка принудительного закрытия клиента:', e.message);
                }
              });
              this.server.close(() => {
                console.log('[P2P] ✅ Сервер закрыт (принудительно)');
                this.server = null;
                this.emit('stopped');
                resolve();
              });
            } catch (error) {
              console.error('[P2P] ⚠️ Критическая ошибка при закрытии сервера:', error.message);
              this.server = null;
              this.emit('stopped');
              resolve();
            }
          } else {
            resolve();
          }
        }, 5000);
        
        this.server.close((error) => {
          clearTimeout(timeout);
          if (error) {
            console.error('[P2P] ⚠️ Ошибка закрытия сервера:', error.message);
          } else {
            console.log('[P2P] ✅ Сервер закрыт');
          }
          this.server = null;
          this.emit('stopped');
          resolve();
        });
      });
    } else {
      this.emit('stopped');
      return Promise.resolve();
    }
  }
  
  /**
   * Статистика узла
   */
  getStats() {
    return {
      nodeId: this.nodeId,
      port: this.port,
      connectedPeers: this.peers.size,
      isRelayServer: this.isRelayServer,
      uptime: process.uptime()
    };
  }

  getTrafficTotals() {
    return { ...this.trafficMetrics };
  }

  setAutoReconnect(address, enabled) {
    if (!address) return;
    this.autoReconnectTargets.set(address, !!enabled);
  }
  
  recordOutgoing(payload) {
    const size = this.computeSize(payload);
    this.trafficMetrics.bytesSent += size;
    this.trafficMetrics.messagesSent += 1;
    this.emit('metrics:traffic', {
      direction: 'out',
      bytes: size,
      messages: 1,
      totals: { ...this.trafficMetrics },
      timestamp: Date.now()
    });
  }

  recordIncoming(data) {
    const size = this.computeSize(data);
    this.trafficMetrics.bytesReceived += size;
    this.trafficMetrics.messagesReceived += 1;
    this.emit('metrics:traffic', {
      direction: 'in',
      bytes: size,
      messages: 1,
      totals: { ...this.trafficMetrics },
      timestamp: Date.now()
    });
  }

  recordError(context, error) {
    this.trafficMetrics.errors += 1;
    this.trafficMetrics.lastError = {
      context,
      message: error && error.message ? error.message : String(error),
      timestamp: Date.now()
    };
    this.emit('metrics:error', {
      context,
      message: this.trafficMetrics.lastError.message,
      timestamp: this.trafficMetrics.lastError.timestamp
    });
  }

  computeSize(payload) {
    if (!payload) return 0;
    if (Buffer.isBuffer(payload)) {
      return payload.length;
    }
    if (typeof payload === 'string') {
      return Buffer.byteLength(payload);
  }
    try {
      return Buffer.byteLength(JSON.stringify(payload));
    } catch (error) {
      return 0;
    }
  }
}

module.exports = { CristallP2PNode };
