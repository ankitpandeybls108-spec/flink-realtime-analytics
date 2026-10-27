@echo off
setlocal
cd /d "%~dp0"

echo =====================================================
echo Flink Real-Time E-Commerce Analytics - startup
echo =====================================================

where mvn >nul 2>&1
if errorlevel 1 (
  echo ERROR: Maven was not found. Open a new CMD and check: mvn -version
  pause
  exit /b 1
)
where docker >nul 2>&1
if errorlevel 1 (
  echo ERROR: Docker CLI was not found. Install Docker Desktop and check: docker --version
  pause
  exit /b 1
)
docker info >nul 2>&1
if errorlevel 1 (
  echo ERROR: Docker Desktop is installed but the Docker engine is not running.
  echo Start Docker Desktop, wait until it says Running, then try again.
  pause
  exit /b 1
)

echo Resetting any existing demo containers for a clean run...
call docker compose down --remove-orphans >nul 2>&1

 echo.
echo [1/4] Building the Java 17 application with Maven...
call mvn -B clean package
if errorlevel 1 (
  echo ERROR: Maven build failed. Read the compiler/build error above.
  pause
  exit /b 1
)

if not exist "flink\lib" mkdir "flink\lib"
copy /Y "target\flink-realtime-analytics-1.0.0.jar" "flink\lib\flink-realtime-analytics-1.0.0.jar" >nul
if errorlevel 1 (
  echo ERROR: Could not copy the built JAR to flink\lib.
  pause
  exit /b 1
)

echo.
echo [2/4] Starting Kafka and the Flink cluster...
call docker compose up -d --remove-orphans
if errorlevel 1 (
  echo ERROR: Docker Compose could not start. Check Docker Desktop and docker-compose.yml.
  pause
  exit /b 1
)

echo.
echo [3/4] Waiting for Kafka to become ready...
set "KAFKA_READY="
for /L %%I in (1,1,45) do (
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >nul 2>&1
  if not errorlevel 1 set "KAFKA_READY=1"
  if defined KAFKA_READY goto kafka_ready
  timeout /t 2 /nobreak >nul
)
echo ERROR: Kafka did not become ready in time. Run: docker compose logs kafka
pause
exit /b 1

:kafka_ready
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic orders --partitions 2 --replication-factor 1
if errorlevel 1 (
  echo ERROR: Could not create the orders Kafka topic.
  pause
  exit /b 1
)

echo.
echo [4/4] Waiting for Flink JobManager, then submitting the job...
set "FLINK_READY="
for /L %%I in (1,1,45) do (
  docker exec flink-jobmanager flink list >nul 2>&1
  if not errorlevel 1 set "FLINK_READY=1"
  if defined FLINK_READY goto flink_ready
  timeout /t 2 /nobreak >nul
)
echo ERROR: Flink JobManager did not become ready in time. Run: docker compose logs jobmanager
pause
exit /b 1

:flink_ready
docker exec flink-jobmanager flink run -d /opt/flink/usrlib/flink-realtime-analytics-1.0.0.jar
if errorlevel 1 (
  echo ERROR: Flink could not submit the JAR. See: docker compose logs jobmanager
  pause
  exit /b 1
)

echo.
echo SUCCESS: Kafka and Flink are running, and the analytics job was submitted.
echo Flink Dashboard: http://localhost:8081
echo.
echo NEXT: Open a second CMD in this project folder and run run-producer.cmd
echo To stop the containers later, run: docker compose down
echo.
pause
