# Финальный проект  по курсу "Apache Kafka для разработки и архитектуры"

## Платформа электронной коммерции

# Базовый функционал
1. Принимать карточки товаров от магазинов
2. Сервис блокировки карточек для доставки пользователям
3. Рекомендация валидных карточек пользователям
4. Пользовательский сервис поиска товаров по карточкам

# Технологический стек
1. bash
2. kafka+zookeeper
3. schema-registry
4. kafka-connect
5. ksqlDB
6. hdfs
7. spark
8. prometheus + grafana
9. python: faust-streaming + confluent-kafka
10. docker compose

# Описание состава контейнеров и сервисов проекта

Сервисы проекта размещаются в 20 контейнерах 

## Скрипт подготовки сертфикатов для SSL-конфигурирования проекта в консоли на хосте

Код [cert.sh](cert.sh)

### zookeeper + kafka-0 + kafka-1 (SSL+SASL+ACL) в Docker
Входящий kafka-кластер
1. Принимает входящие карточки товара в очередь goods через shop-api
2. Отдает входяще карточки на фильтрацию и принимает валидные в очередь goods-filtered

### zookeeper-mirror + kafka-mirror-0 + kafka-mirror-1 (SSL+SASL+ACL) в Docker
Аналитический kafka-кластер
1. Зеркалирует на себя очередь goods-filtered в source.goods-filtered
2. Отдает топик source.goods-filtered в аналитический spark-hdfs блок
3. Принимает от аналитического блока в топик goods-recomendations карточки для рекомендации пользователям
4. Предоставляет user-api возможность чтения рекомендованных карточек и поиск по валидным карточкам
5. Принимает в топик events события запросов от user-api

### ui в Docker
UI для обоих kafka-кластеров и schema-registry

### shop-api в Docker
Эмулирует входящий трафик карточек от магазинов. Реализация на python confluent-kafka 

Код: [shop_api/app.py](shop_api/app.py)

Регистриует  схемы для всех товарных топиков в schema-registry

Раз в 5сек формирует по образцу одну из 5-ти каточек товара, назначает ей случайную цену и отправляет в топик goods

### blocking в Docker
Фильтрует карточки от магазинов. Реализация на python faust-streaming

Код: [blocking/app.py](blocking/app.py)

Вычитывает очередь goods и передает в goods-filtered сообщения с валидными product_id

### schema-registry в Docker
Хранит схемы товарных топиков обоих  обоих kafka-кластеров

### hdfs-nn + hdfs-dn-0 +  + hdfs-dn-1  в Docker
Файловая система для spark-hdfs аналитического блока

### spark-master + spark-worker в Docker
Движок для spark-hdfs аналитического блока

### ksqldb в Docker
Движок kSQLDB для аналитического kafka-кластера

### kafka-connect в Docker
Сервис коннекторов для kafka-кластеров

Коннекторы:

1. MirrorSourceConnector
    Зеркалирование топика и ACL goods-filtered входящего кластера в source.goods-filtered аналитического

    Конфиг: [connect-config/mm2-source.json](connect-config/mm2-source.json)

2. HdfsSinkConnector
    Sink топика source.goods-filtered аналитического кластера в hdfs

    Конфиг: [connect-config/hdfs-sink.json](connect-config/hdfs-sink.json)    

### kafka-init в Docker
Сервис инициализации кластера проекта. Запускается при старте docker-compose проекта, отрабатывает и выключается

Код: [kafka-init.sh](kafka-init.sh)

Задачи:
1. Регистрация топиков входящего и аналитического kafka-кластеров
2. Регистрация ACL входящего и аналитического kafka-кластеров
2. Запуск коннекторов kafka-connect

### spark-app в Docker
Запускает приложение в spark-hdfs аналитическом блоке

Код: [spark/app.py](spark/app.py)

Эмулирует работу рекомендательной системы: вычитывает sink топика source.goods-filtered из hdfs батчами по одному файлу, каждый 10-й батч отправляет в goods-recomendations в качестве рекомендации

### user-api в Docker в консоли на хосте
Эмуляция сервиса для поиска и получения рекомендация пользователей + запись обращений к сервису в топик event аналитического kafka-кластера

Код: [user_api](user_api)

Формат использования

1. Чтение рекомендация из топика goods-recomendations (confluent_kafka)
  
```
user_api$python app.py recommendations
========================================
======New recomendation [0:9] =========
========================================
{'brand': 'brand_GHI',
 'category': 'Электроника',
 'created_at': '2023-10-01T12:00:00Z',
 'description': 'Умные часы с функцией мониторинга здоровья, GPS и '
                'уведомлениями за 7059.72 RUB',
 'images': [{'alt': 'Умные часы XYZ - вид спереди',
             'url': 'https://example.com/images/product1.jpg'},
            {'alt': 'Умные часы XYZ - вид сбоку',
             'url': 'https://example.com/images/product1_side.jpg'}],
 'index': 'products',
 'name': 'Умные часы GHI',
 'price': {'amount': 7059.72, 'currency': 'RUB'},
 'product_id': '12347',
 'sku': 'XYZ-12345',
 'stock': {'available': 150, 'reserved': 20},
 'store_id': 'store_001',
 'tags': ['умные часы', 'гаджеты', 'технологии'],
 'updated_at': '2026-01-20T11:11:07Z'}
========================================
======New recomendation [1:16] =========
========================================
{'brand': 'brand_JKL',
 'category': 'Электроника',
 'created_at': '2023-10-01T12:00:00Z',
 'description': 'Умные часы с функцией мониторинга здоровья, GPS и '
                'уведомлениями за 5091.02 RUB',
 'images': [{'alt': 'Умные часы XYZ - вид спереди',
             'url': 'https://example.com/images/product1.jpg'},
            {'alt': 'Умные часы XYZ - вид сбоку',
             'url': 'https://example.com/images/product1_side.jpg'}],
 'index': 'products',
 'name': 'Умные часы JKL',
 'price': {'amount': 5091.02, 'currency': 'RUB'},
 'product_id': '12348',
 'sku': 'XYZ-12345',
 'stock': {'available': 150, 'reserved': 20},
 'store_id': 'store_001',
 'tags': ['умные часы', 'гаджеты', 'технологии'],
 'updated_at': '2026-01-20T11:11:02Z'}
.......
^C
There was 9 messages
```

2. Поиск текущей цены на товары по имени и бренду (kSQLDB+stream+table)

```
user_api$python app.py search --name JKL
Поиск по имени:  JKL
PRODUCT_ID           | NAME                 | BRAND                | PRICE_STR           
12348                | Умные часы JKL       | brand_JKL            | 9763.86 RUB         

user_api$python app.py search --brand d_JKL
Поиск по бренду: d_JKL
PRODUCT_ID           | NAME                 | BRAND                | PRICE_STR           
12348                | Умные часы JKL       | brand_JKL            | 9763.86 RUB      
```

### prometheus + grafana в Docker 
Мониторинг работы коннекторов kafka-connect

![](/ss/ss.png)