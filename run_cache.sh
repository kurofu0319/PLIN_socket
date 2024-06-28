start_time=$(date +%s%3N)

echo "Running script..."

./test_client --port=8080 --number=5e7 --test_size=5e7 --index_type=plin_cache --batch_size=10000 --key_distribution=lognormal --upsert_ratio=0 &
./test_client --port=8080 --number=5e7 --test_size=5e7 --index_type=plin_cache --batch_size=10000 --key_distribution=lognormal --upsert_ratio=0 &
./test_client --port=8080 --number=5e7 --test_size=5e7 --index_type=plin_cache --batch_size=10000 --key_distribution=lognormal --upsert_ratio=0 &
./test_client --port=8080 --number=5e7 --test_size=5e7 --index_type=plin_cache --batch_size=10000 --key_distribution=lognormal --upsert_ratio=0 

wait

end_time=$(date +%s%3N)

duration=$((end_time - start_time))

echo "Script completed in ${duration} milliseconds."
