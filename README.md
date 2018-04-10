## 메시지 전송
queue 서버에 메시지를 전송하면 worker들이 해당 작업을 실행합니다.

```
$ cd ntrust-backend/oneshot

ping test  
$ python3 mq_send_single.py ping  

update worker  
$ python3 mq_send_single.py update  

뉴스데이터 처리  
$ python3 mq_send_single.py byline 02100311.20160620110049861   
$ python3 mq_send_single.py basic 02100311.20160620110049861   
$ python3 mq_send_single.py score 02100311.20160620110049861   
```