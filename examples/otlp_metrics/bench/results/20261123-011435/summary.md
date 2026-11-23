# OTLP Export benchmark summary

Medians over the repetitions (min-max in brackets). Generated from runs/*.json by run_bench.py; see environment.json for the machine, builds and settings. "client non-OK" are calls in flight when ghz's duration ended (cancelled or cut off by ghz); "server errors" are error statuses the receiver returned (CANCELLED excluded).

## Workload `small`: 2061 bytes, 10 data points (0 rejected) per request

| configuration | req/s | accepted points/s | p50 ms | p90 ms | p99 ms | server CPU cores | CPU µs/req | peak RSS MiB | client non-OK | server errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ezgrpc-memory-loops4 | 38,579 [38,517-38,632] | 385,793 | 0.35 | 1.28 | 3.11 | 1.20 | 31.1 | 362 | 20 | 0 |
| ezgrpc-mimalloc-null-loops1 | 38,800 [38,738-38,878] | 388,008 | 0.39 | 1.27 | 3.05 | 0.81 | 21.0 | 8 | 28 | 0 |
| ezgrpc-mimalloc-null-loops4 | 38,645 [38,584-38,772] | 386,458 | 0.35 | 1.27 | 3.08 | 1.20 | 31.0 | 10 | 37 | 0 |
| ezgrpc-null-loops1 | 38,851 [38,750-38,918] | 388,517 | 0.40 | 1.28 | 3.10 | 0.82 | 21.1 | 7 | 28 | 0 |
| ezgrpc-null-loops1-workers4 | 38,599 [38,435-38,630] | 385,997 | 0.38 | 1.28 | 3.12 | 1.23 | 32.0 | 7 | 22 | 0 |
| ezgrpc-null-loops2 | 38,727 [38,718-38,787] | 387,278 | 0.35 | 1.26 | 3.04 | 1.03 | 26.6 | 7 | 19 | 0 |
| ezgrpc-null-loops4 | 38,703 [38,498-38,719] | 387,034 | 0.35 | 1.26 | 3.06 | 1.24 | 32.0 | 7 | 18 | 0 |
| ezgrpc-wal-fsync-loops4 | 27,802 [6,240-28,158] | 278,053 | 1.75 | 2.40 | 4.26 | 0.97 | 35.5 | 8 | 129 | 0 |
| grpcgo-memory-procs4 | 37,900 [37,870-37,936] | 379,016 | 0.67 | 1.67 | 3.38 | 3.30 | 87.0 | 629 | 71 | n/a |
| grpcgo-null-procs1 | 16,839 [16,780-16,842] | 168,412 | 3.65 | 4.65 | 5.27 | 0.99 | 58.9 | 18 | 116 | n/a |
| grpcgo-null-procs2 | 28,224 [27,703-28,593] | 282,278 | 1.66 | 2.72 | 4.29 | 1.89 | 66.9 | 19 | 147 | n/a |
| grpcgo-null-procs4 | 37,113 [36,757-37,158] | 371,155 | 0.93 | 1.88 | 3.41 | 3.40 | 91.7 | 20 | 118 | n/a |
| sat-ezgrpc-mimalloc-null-loops1 | 58,821 [57,510-58,983] | 588,218 | 0.68 | 0.91 | 1.80 | 0.92 | 15.7 | 8 | 103 | 0 |
| sat-ezgrpc-null-loops1 | 56,916 [56,490-57,045] | 569,199 | 0.74 | 0.94 | 1.78 | 0.93 | 16.4 | 8 | 97 | 0 |
| sat-grpcgo-null-procs1 | 16,669 [16,146-16,674] | 166,740 | 3.91 | 4.95 | 5.81 | 0.99 | 59.4 | 18 | 188 | n/a |

## Workload `large`: 175368 bytes, 1000 data points (0 rejected) per request

| configuration | req/s | accepted points/s | p50 ms | p90 ms | p99 ms | server CPU cores | CPU µs/req | peak RSS MiB | client non-OK | server errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ezgrpc-memory-loops4 | 1,273 [1,271-1,279] | 1,273,396 | 42.12 | 63.99 | 84.07 | 1.25 | 973.7 | 384 | 149 | 0 |
| ezgrpc-mimalloc-null-loops1 | 1,119 [1,114-1,122] | 1,119,348 | 56.41 | 66.76 | 77.69 | 0.92 | 825.2 | 22 | 189 | 0 |
| ezgrpc-mimalloc-null-loops4 | 1,277 [1,275-1,278] | 1,277,185 | 42.31 | 63.38 | 83.08 | 1.28 | 1004.1 | 39 | 137 | 0 |
| ezgrpc-null-loops1 | 1,121 [1,121-1,121] | 1,121,368 | 56.30 | 66.78 | 77.66 | 0.92 | 823.2 | 21 | 186 | 0 |
| ezgrpc-null-loops1-workers4 | 1,280 [1,277-1,282] | 1,280,007 | 30.10 | 55.60 | 83.84 | 1.38 | 1077.7 | 29 | 90 | 0 |
| ezgrpc-null-loops2 | 1,259 [1,203-1,274] | 1,259,457 | 47.26 | 63.10 | 77.68 | 1.23 | 974.1 | 23 | 176 | 0 |
| ezgrpc-null-loops4 | 1,275 [1,273-1,279] | 1,275,336 | 41.94 | 63.69 | 82.98 | 1.30 | 1023.4 | 28 | 177 | 0 |
| grpcgo-memory-procs4 | 1,263 [1,257-1,281] | 1,263,195 | 27.83 | 58.51 | 103.53 | 3.32 | 2635.7 | 581 | 74 | n/a |
| grpcgo-null-procs1 | 422 [419-423] | 423,515 | 92.74 | 321.95 | 441.45 | 1.00 | 2337.9 | 34 | 162 | n/a |
| grpcgo-null-procs2 | 785 [776-786] | 787,247 | 65.40 | 130.06 | 311.72 | 1.99 | 2509.2 | 49 | 175 | n/a |
| grpcgo-null-procs4 | 1,230 [1,220-1,232] | 1,230,317 | 38.76 | 73.78 | 121.93 | 3.94 | 3186.7 | 71 | 118 | n/a |
| sat-ezgrpc-mimalloc-null-loops1 | 1,175 [1,175-1,175] | 1,175,564 | 53.92 | 62.08 | 70.98 | 0.99 | 844.7 | 23 | 188 | 0 |
| sat-ezgrpc-null-loops1 | 1,175 [1,173-1,179] | 1,175,704 | 53.91 | 62.40 | 71.97 | 0.99 | 844.4 | 21 | 190 | 0 |
| sat-grpcgo-null-procs1 | 425 [425-426] | 428,379 | 108.46 | 300.95 | 455.92 | 1.00 | 2310.0 | 34 | 192 | n/a |

## Workload `partial`: 3458 bytes, 20 data points (5 rejected) per request

| configuration | req/s | accepted points/s | p50 ms | p90 ms | p99 ms | server CPU cores | CPU µs/req | peak RSS MiB | client non-OK | server errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ezgrpc-memory-loops4 | 30,886 [30,843-30,945] | 463,312 | 0.50 | 1.63 | 3.87 | 1.50 | 48.7 | 366 | 54 | 0 |
| ezgrpc-mimalloc-null-loops1 | 30,983 [30,960-31,017] | 464,760 | 0.53 | 1.64 | 3.84 | 0.83 | 26.8 | 8 | 33 | 0 |
| ezgrpc-mimalloc-null-loops4 | 30,892 [30,881-31,000] | 463,398 | 0.46 | 1.61 | 3.80 | 1.17 | 37.8 | 10 | 29 | 0 |
| ezgrpc-null-loops1 | 30,988 [30,948-30,996] | 464,841 | 0.54 | 1.65 | 3.79 | 0.84 | 27.0 | 7 | 31 | 0 |
| ezgrpc-null-loops1-workers4 | 30,787 [30,767-30,804] | 461,812 | 0.47 | 1.63 | 3.77 | 1.28 | 41.6 | 7 | 43 | 0 |
| ezgrpc-null-loops2 | 30,904 [30,870-30,931] | 463,576 | 0.48 | 1.62 | 3.76 | 1.11 | 35.7 | 7 | 28 | 0 |
| ezgrpc-null-loops4 | 30,852 [30,840-30,915] | 462,795 | 0.46 | 1.62 | 3.85 | 1.22 | 39.4 | 7 | 20 | 0 |
| grpcgo-memory-procs4 | 29,542 [29,329-29,567] | 443,169 | 1.25 | 2.45 | 4.51 | 3.60 | 122.0 | 615 | 98 | n/a |
| grpcgo-null-procs1 | 12,423 [12,299-12,444] | 186,447 | 4.75 | 6.12 | 7.61 | 0.99 | 80.0 | 19 | 156 | n/a |
| grpcgo-null-procs2 | 20,843 [20,574-21,084] | 312,694 | 2.33 | 3.98 | 5.81 | 1.91 | 91.8 | 19 | 136 | n/a |
| grpcgo-null-procs4 | 28,169 [28,151-28,241] | 422,564 | 1.40 | 2.77 | 4.58 | 3.52 | 124.5 | 20 | 110 | n/a |
| sat-ezgrpc-mimalloc-null-loops1 | 44,355 [44,248-44,500] | 665,378 | 1.02 | 1.26 | 2.24 | 0.95 | 21.3 | 8 | 133 | 0 |
| sat-ezgrpc-null-loops1 | 42,955 [42,696-42,995] | 644,405 | 1.08 | 1.31 | 2.22 | 0.95 | 22.1 | 8 | 85 | 0 |
| sat-grpcgo-null-procs1 | 12,189 [12,182-12,221] | 182,853 | 5.10 | 6.28 | 7.96 | 0.99 | 81.3 | 19 | 54 | n/a |
