import pika 
import sys 

class Youtuber:
    def __init__(self,name):
        self.name=name
        
        
        # Establish connection
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('35.225.139.226'))
        self.channel = self.connection.channel()
        
        #declare queues 
        self.video_queue=self.channel.queue_declare(queue='video_queue')
        
        
         
        
        
    def publish_video(self, youtuber_name, video_name ):
       #code
       self.channel.basic_publish(exchange='',routing_key='video_queue', body=f"{youtuber_name}:{video_name}")
       print("SUCCESS: Video sent to youtuber server. ") 
       

if __name__=="__main__":
    if len(sys.argv) < 3:
        print("Please add video in correct format")
        sys.exit(1)
        
        
    youtuber_name=sys.argv[1]
    video_name=' '.join(sys.argv[2:])
    
    youtuber=Youtuber(youtuber_name)
    
    youtuber.publish_video(youtuber_name,video_name)
    