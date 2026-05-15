import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  stages: [
    { duration: '1m', target: 50 },  # ramp up to 50 users
    { duration: '3m', target: 200 }, # stay at 200 users (simulating 200 factories)
    { duration: '1m', target: 0 },   # ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<200'], # 95% of requests must complete below 200ms
  },
};

export default function () {
  const url = 'http://localhost:8002/api/v1/machines';
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer MOCK_TOKEN',
    },
  };

  let res = http.get(url, params);
  
  check(res, {
    'is status 200': (r) => r.status === 200,
    'latency is acceptable': (r) => r.timings.duration < 500,
  });

  sleep(0.1); # 10 requests per second per user
}
