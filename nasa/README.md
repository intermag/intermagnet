nasa文件夹下定义了  https://spdf.gsfc.nasa.gov/pub/data/   的爬虫，爬虫爬取cdf文件后保存到本地文件夹，并将文件以及文件下载链接等信息保存到momgoDB中

存储数据之后，将数据信息以dict形式发送到rabbitmq server。消息对应的消费者为rqworker文件夹下的nasacdf_worker.py

目前该消费者只是初步地对生产者的消息进行接收

启动爬虫需要cd到nasa文件夹下，输入scrapy crawl nasacdf，便可以运行
