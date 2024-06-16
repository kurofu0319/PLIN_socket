start_time=$(date +%s)

echo "Running script..."

./test_client 8080 1e8 1e8 0 100 &
./test_client 8080 1e8 1e8 0 100 &
./test_client 8080 1e8 1e8 0 100 &
./test_client 8080 1e8 1e8 0 100 &
./test_client 8080 1e8 1e8 0 100 &
./test_client 8080 1e8 1e8 2 100 &
./test_client 8080 1e8 1e8 2 100 &
./test_client 8080 1e8 1e8 2 100 &
./test_client 8080 1e8 1e8 2 100 &
./test_client 8080 1e8 1e8 2 100 &
./test_client 8080 1e8 1e8 0 100 &
./test_client 8080 1e8 1e8 0 100 &
./test_client 8080 1e8 1e8 0 100 &
./test_client 8080 1e8 1e8 0 100 &
./test_client 8080 1e8 1e8 0 100 &
./test_client 8080 1e8 1e8 2 100 &
./test_client 8080 1e8 1e8 2 100 &
./test_client 8080 1e8 1e8 2 100 &
./test_client 8080 1e8 1e8 2 100 &
./test_client 8080 1e8 1e8 2 100 



wait


end_time=$(date +%s)

duration=$((end_time - start_time))

echo "Script completed in ${duration} seconds."
