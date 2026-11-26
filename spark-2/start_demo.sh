#!/bin/bash

echo " MUSIC STREAMING ANALYTICS DEMO "
echo " ============================== "

#Activate Virtual Environment

source venv/bin/activate

#start kafka with proper waiting
echo -e "\n1. starting kafka..."
docker-compose down 2>/dev/null #clean up any existing
docker-compose up -d
#wait for kafka to be ready
echo -e "\n2. Waiting for kafka to start... "
sleep 15

#Check if kafka is responding 

echo -e "\n3. Checking for kafka connection...  "
for i in {1..10}; do
    if docker-compose exec broker kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        echo " Kafka is ready"
        break
    else
        echo " % Waiting for kafka.... {attempt $i/10}"
        sleep 3
    fi
    if [$i -eq 10 ]; then
        echo " % kafka failed to start Check docker-compose logs. "
        docker-compose logs
        exit 1
    fi
done

#create Topic
echo -e "\n5. Ready to start!"
echo -e "\n Open 3 terminals"
echo ""
echo " Terminal 1 : kafka is running here"
echo " Keep this terminal open"
echo " View Logs: docker-compose logs -f"
echo ""
echo " Terminal 2: Run the music streaming producer"
echo "cd $(pwd)"
echo "source venv/bin/activate"
echo " python producer.py"
echo ""
echo "Terminal 3: Run the music analytics dashboard"
echo "cd $(pwd)"
echo "source venv/bin/activate"
echo "python dashboard.py"
echo ""
echo "============================================="
echo "Music analytics Features:"
echo " Top Artists"
echo "Genre distribution"
echo "User listening stats"
echo "Real time trends"
echo "Most played artists"
echo ""
echo "After dashboard starts. press ctrl+c to enter interactivate mode!"
echo "=============================================="

#show kafka logs
docker-compose logs -f 