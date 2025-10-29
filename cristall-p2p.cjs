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

class CristallP2PNode extends EventEmitter {
  constructor(options = {}) {
    super();
    
    this.nodeId = options.nodeId || this.generateNodeId();
    this.port = options.port || 7777;
    this.isRelayServer = options.isRelayServer || false;
    
    // Хранилище подключений
    this.peers = new Map(); // peerId -> { ws, metadata }
    this.knownNodes = new Set(); // Список известных адресов
    
    // WebSocket сервер (если мы relay или обычный узел)
    this.server = null;
    
    // WebSocket клиент (подключения к другим узлам)
    this.connections = new Map(); // address -> ws
    
    // Настройки
    this.reconnectInterval = options.reconnectInterval || 5000;
    this.heartbeatInterval = options.heartbeatInterval || 30000;
    
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
    // Создаём WebSocket сервер
    this.server = new WebSocket.Server({ port: this.port });
    console.log(`[P2P] 🌐 Сервер запущен на порту ${this.port}`);
    
    // Обработка входящих подключений
    this.server.on('connection', (ws, req) => {
      const address = req.socket.remoteAddress;
      console.log(`[P2P] ✅ Входящее подключение от ${address}`);
      
      this.handleIncomingConnection(ws, address);
    });
    
    // Запуск heartbeat (проверка живости соединений)
    this.startHeartbeat();
    
    this.emit('started', { nodeId: this.nodeId, port: this.port });
  }
  
  /**
   * Обработка входящего подключения
   */
  handleIncomingConnection(ws, address) {
    let peerId = null;
    
    ws.on('message', (data) => {
      try {
        const message = JSON.parse(data.toString());
        
        // Первое сообщение - handshake (обмен ID)
        if (message.type === 'handshake') {
          peerId = message.nodeId;
          this.peers.set(peerId, { ws, address, metadata: message.metadata });
          
          // Отправляем свой ID
          ws.send(JSON.stringify({
            type: 'handshake_ack',
            nodeId: this.nodeId,
            metadata: this.getNodeMetadata()
          }));
          
          console.log(`[P2P] 🤝 Handshake завершён с ${peerId}`);
          this.emit('peer:connected', { peerId, address });
        }
        // Обработка других сообщений
        else {
          this.handleMessage(message, peerId);
        }
      } catch (error) {
        console.error(`[P2P] ❌ Ошибка обработки сообщения:`, error.message);
      }
    });
    
    ws.on('close', () => {
      if (peerId) {
        this.peers.delete(peerId);
        console.log(`[P2P] ❌ Узел отключился: ${peerId}`);
        this.emit('peer:disconnected', { peerId });
      }
    });
    
    ws.on('error', (error) => {
      console.error(`[P2P] ⚠️ Ошибка соединения:`, error.message);
    });
  }
  
  /**
   * Подключение к другому узлу
   */
  async connectToPeer(address) {
    if (this.connections.has(address)) {
      console.log(`[P2P] ⚠️ Уже подключены к ${address}`);
      return;
    }
    
    console.log(`[P2P] 🔌 Подключаемся к ${address}...`);
    
    try {
      const ws = new WebSocket(address);
      
      ws.on('open', () => {
        console.log(`[P2P] ✅ Подключение установлено: ${address}`);
        
        // Отправляем handshake
        ws.send(JSON.stringify({
          type: 'handshake',
          nodeId: this.nodeId,
          metadata: this.getNodeMetadata()
        }));
        
        this.connections.set(address, ws);
      });
      
      ws.on('message', (data) => {
        try {
          const message = JSON.parse(data.toString());
          
          if (message.type === 'handshake_ack') {
            const peerId = message.nodeId;
            this.peers.set(peerId, { ws, address, metadata: message.metadata });
            console.log(`[P2P] 🤝 Handshake ACK от ${peerId}`);
            this.emit('peer:connected', { peerId, address });
          } else {
            this.handleMessage(message, message.from);
          }
        } catch (error) {
          console.error(`[P2P] ❌ Ошибка обработки сообщения:`, error.message);
        }
      });
      
      ws.on('close', () => {
        console.log(`[P2P] ❌ Соединение закрыто: ${address}`);
        this.connections.delete(address);
        
        // Автопереподключение
        setTimeout(() => {
          console.log(`[P2P] 🔄 Попытка переподключения к ${address}...`);
          this.connectToPeer(address);
        }, this.reconnectInterval);
      });
      
      ws.on('error', (error) => {
        console.error(`[P2P] ⚠️ Ошибка подключения к ${address}:`, error.message);
      });
      
    } catch (error) {
      console.error(`[P2P] ❌ Не удалось подключиться к ${address}:`, error.message);
    }
  }
  
  /**
   * Обработка входящих сообщений
   */
  handleMessage(message, fromPeerId) {
    console.log(`[P2P] 📨 Получено сообщение от ${fromPeerId}:`, message.type);
    
    // Если сообщение для relay (пересылка)
    if (this.isRelayServer && message.to && message.to !== this.nodeId) {
      this.relayMessage(message);
      return;
    }
    
    // Обработка различных типов сообщений
    switch (message.type) {
      case 'ping':
        this.sendToPeer(fromPeerId, { type: 'pong', timestamp: Date.now() });
        break;
        
      case 'peer_discovery':
        // Отправляем список известных узлов
        this.sendToPeer(fromPeerId, {
          type: 'peer_list',
          peers: this.getKnownPeers()
        });
        break;
        
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
        
      default:
        // Пользовательские сообщения
        this.emit('message', { from: fromPeerId, message });
    }
  }
  
  /**
   * Relay - пересылка сообщения другому узлу
   */
  relayMessage(message) {
    const targetPeer = this.peers.get(message.to);
    
    if (targetPeer && targetPeer.ws.readyState === WebSocket.OPEN) {
      targetPeer.ws.send(JSON.stringify(message));
      console.log(`[P2P] 🔄 Relay: ${message.from} → ${message.to}`);
    } else {
      console.log(`[P2P] ⚠️ Relay failed: узел ${message.to} не найден`);
    }
  }
  
  /**
   * Отправка сообщения конкретному узлу
   */
  sendToPeer(peerId, message) {
    const peer = this.peers.get(peerId);
    
    if (!peer) {
      console.error(`[P2P] ❌ Узел ${peerId} не найден`);
      return false;
    }
    
    if (peer.ws.readyState !== WebSocket.OPEN) {
      console.error(`[P2P] ❌ Соединение с ${peerId} не активно`);
      return false;
    }
    
    const envelope = {
      ...message,
      from: this.nodeId,
      timestamp: Date.now()
    };
    
    peer.ws.send(JSON.stringify(envelope));
    return true;
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
      timestamp: Date.now()
    };
  }
  
  /**
   * Получение списка известных узлов
   */
  getKnownPeers() {
    const peers = [];
    this.peers.forEach((peer, peerId) => {
      peers.push({
        peerId,
        address: peer.address,
        metadata: peer.metadata
      });
    });
    return peers;
  }
  
  /**
   * Heartbeat - проверка живости соединений
   */
  startHeartbeat() {
    setInterval(() => {
      this.peers.forEach((peer, peerId) => {
        if (peer.ws.readyState === WebSocket.OPEN) {
          this.sendToPeer(peerId, { type: 'ping' });
        }
      });
    }, this.heartbeatInterval);
  }
  
  /**
   * Остановка узла
   */
  async stop() {
    console.log('[P2P] 🛑 Остановка узла...');
    
    // Закрываем все подключения
    this.peers.forEach((peer) => peer.ws.close());
    this.connections.forEach((ws) => ws.close());
    
    // Останавливаем сервер
    if (this.server) {
      this.server.close();
    }
    
    this.emit('stopped');
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
}

module.exports = { CristallP2PNode };

