start_time=$(date +%s)

echo "Running script..."

./test_client 8080 1e7 1e6 1 &
./test_client 8080 1e7 1e6 1 &
./test_client 8080 1e7 1e6 1 &
./test_client 8080 1e7 1e6 1 &
./test_client 8080 1e7 1e6 1 &
./test_client 8080 1e7 1e6 1 &
./test_client 8080 1e7 1e6 1 &
./test_client 8080 1e7 1e6 1 &
./test_client 8080 1e7 1e6 1 &
./test_client 8080 1e7 1e6 1 

end_time=$(date +%s)

duration=$((end_time - start_time))

echo "Script completed in ${duration} seconds."