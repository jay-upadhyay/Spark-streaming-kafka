from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'jay'

with open("/home/cloudera/Desktop/HW_5/orders.txt") as fh:
	count=0
	for line in fh:
		count+=1
		producer.send(topic,line)
		print(line)
		if count%1000==0:
			time.sleep(1)
print 'Done sending messages'
