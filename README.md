# log-collector (통합로그수집)


### Feature

- 다양한 프로토콜 기반 로그 수집
- 주기적 수집 (Polling), 데이터 수신 (Push Receive) 방식 지원
- 각 프로토콜에 맞는 설정 입력을 통한 실시간 수집 관리
- Push/Polling 인터페이스 구현을 통한 추가 프로토콜 확대 가능



#### Support Protocol

 - Polling (Scheduling)
    -  Ftp Directory Scan (File)
    - Jdbc SQL Execute
    - Local Directory Scan (File)
    - Local File Tail
    - Local Shell Executor
    - Sftp Directory Scan (File)
    - SNMP
    - SSH Connect and Shell Executor
 - Push Receive
    - SNMP Trap
    - Syslog
    - Tcp Packet