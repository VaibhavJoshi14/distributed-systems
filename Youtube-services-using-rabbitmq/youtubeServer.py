import pika 
import threading

class YoutubeServer:
    def __init__(self):
        print("")
        
        #establish connnection
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel=self.connection.channel()
        
        #declare queues 
        # 1. video queue 
        self.video_queue=self.channel.queue_declare(queue='video_queue')
        #2. user queue 
        self.video_queue=self.channel.queue_declare(queue='user_queue')
        
        #Subscribed users 
        self.user_subscriptions={} 
        
        print("Youtube Server is Running ")
             

              
        
    def start_consuming_request(self):
        
        def consume_youtuber_request(ch, method,properties,body):
            youtuber_name, video_name = body.decode().split(':')
            print(f"{youtuber_name} uploaded {video_name}")
            self.notify_user(youtuber_name, video_name)
            
            
            
            
            
        def consume_user_request(ch,method,properties,body):
            username, youtuber_name, action = body.decode().split(':')
            if action=='subscribe':
                print(f"{username} subscribed to {youtuber_name}")
                
                if youtuber_name not in self.user_subscriptions:
                    self.user_subscriptions[youtuber_name]=set()
                
                self.user_subscriptions[youtuber_name].add(username)
            elif action == 'unsubscribe':
                print(f"{username} unsubscribed from {youtuber_name}")
                
                if youtuber_name not in self.user_subscriptions:
                    self.user_subscriptions[youtuber_name]=set()
                
                self.user_subscriptions[youtuber_name].discard(username)
            else:
                print("what the hell is happeing ??")
        
        print("YoutubeServer is now consuming YouTuber requests...")
        self.channel.basic_consume(queue='video_queue', on_message_callback=consume_youtuber_request, auto_ack=True)
        
        
        print("YoutubeServer is now consuming User requests...")
        self.channel.basic_consume(queue='user_queue', on_message_callback=consume_user_request, auto_ack=True)
        
        self.channel.start_consuming()
            
    def notify_user(self, youtuber_name, video_name):
        print("notifying...")
        if youtuber_name in self.user_subscriptions:
            subscribed_user=self.user_subscriptions[youtuber_name]
            for username in subscribed_user:
                self.channel.queue_declare(queue=f'{username}_notifications')
                message=f'{video_name} uploaded by {youtuber_name}'
                self.channel.basic_publish(exchange='',routing_key=f'{username}_notifications', body=message)
            
            
            
                
        
        


if __name__=="__main__":
    youtube_server=YoutubeServer()
    
    youtube_server.start_consuming_request()

    
    youtube_server.connection.close()
    
    
    