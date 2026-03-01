const CACHE_NAME = 'aihub-news-cache-v3';
const STATIC_ASSETS = [
  './',
  './index.html',
  './icon.png',
  './logo.png',
  './manifest.webmanifest'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(STATIC_ASSETS)).catch(() => Promise.resolve())
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys.map((key) => {
          if (key !== CACHE_NAME) return caches.delete(key);
          return Promise.resolve();
        })
      )
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);
  const isApi = url.pathname.startsWith('/api/');
  if (isApi) {
    event.respondWith(
      fetch(req).catch(() =>
        caches.match('./index.html').then(
          (fallback) =>
            fallback ||
            new Response(JSON.stringify({ ok: false, error: 'offline' }), {
              status: 503,
              headers: { 'Content-Type': 'application/json' }
            })
        )
      )
    );
    return;
  }

  event.respondWith(
    caches.match(req).then(
      (cached) =>
        cached ||
        fetch(req)
          .then((networkRes) => {
            const clone = networkRes.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(req, clone)).catch(() => {});
            return networkRes;
          })
          .catch(() => caches.match('./index.html'))
    )
  );
});
