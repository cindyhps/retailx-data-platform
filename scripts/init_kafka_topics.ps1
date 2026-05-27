# scripts/init_kafka_topics.ps1

$KAFKA_CONTAINER = "retailx-data-platform-kafka-1"
$BOOTSTRAP = "localhost:9092"

Write-Host "Membuat Kafka topics..."

$topics = @("retail.orders", "retail.order_items", "retail.payments")

foreach ($topic in $topics) {
    docker exec $KAFKA_CONTAINER `
        kafka-topics --create `
        --bootstrap-server $BOOTSTRAP `
        --topic $topic `
        --partitions 3 `
        --replication-factor 1 `
        --if-not-exists

    Write-Host "Topic $topic berhasil dibuat"
}

Write-Host "Semua topics siap."