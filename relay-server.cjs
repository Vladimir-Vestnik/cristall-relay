const { CristallP2PNode } = require('./cristall-p2p.cjs');

async function main() {
  console.log('🚀 Запуск Cristall Relay Server на GitHub Codespaces...');
  
  const PORT = process.env.PORT || 7777;
  
  const relayNode = new CristallP2PNode({
    port: PORT,
    isRelayServer: true
  });

  await relayNode.start();
  
  console.log('✅ Relay Server запущен!');
  console.log(`📡 Локальный порт: ${PORT}`);
  console.log(`🆔 NodeId: ${relayNode.nodeId}`);
  console.log('');
  console.log('💡 Для публичного доступа открой порт в Codespaces:');
  console.log('   1. Перейди на вкладку PORTS');
  console.log('   2. Найди порт 7777');
  console.log('   3. Сделай его Public (правая кнопка → Port Visibility → Public)');
  console.log('   4. Скопируй Forwarded Address');
  console.log('');
  
  relayNode.on('peer:connected', (peerId, metadata) => {
    console.log(`✅ [${new Date().toISOString()}] Peer подключён: ${peerId}`);
    console.log(`   Metadata:`, metadata);
    console.log(`   Всего peers: ${relayNode.getKnownPeers().length}`);
  });
  
  relayNode.on('peer:disconnected', (peerId) => {
    console.log(`❌ [${new Date().toISOString()}] Peer отключён: ${peerId}`);
    console.log(`   Осталось peers: ${relayNode.getKnownPeers().length}`);
  });
  
  // Keep alive - печатаем статус каждые 30 секунд
  setInterval(() => {
    const peers = relayNode.getKnownPeers();
    console.log(`📊 Статус: ${peers.length} активных узлов`);
    if (peers.length > 0) {
      peers.forEach(peer => {
        console.log(`   - ${peer.peerId.substring(0, 8)}... (${peer.address})`);
      });
    }
  }, 30000);
}

main().catch(err => {
  console.error('❌ Ошибка запуска relay:', err);
  process.exit(1);
});


