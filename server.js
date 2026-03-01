const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { URL } = require('url');

function loadDotEnv() {
  const envPath = path.join(__dirname, '.env');
  if (!fs.existsSync(envPath)) return;

  const lines = fs.readFileSync(envPath, 'utf8').split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const separator = trimmed.indexOf('=');
    if (separator <= 0) continue;

    const key = trimmed.slice(0, separator).trim();
    const value = trimmed.slice(separator + 1).trim();
    if (key && process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

loadDotEnv();

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '127.0.0.1';
const ROOT_DIR = __dirname;
const DATA_DIR = path.join(ROOT_DIR, 'data');
const MANUAL_NEWS_FILE = path.join(DATA_DIR, 'manual-news.json');
const SUMMARY_CACHE_FILE = path.join(DATA_DIR, 'summary-cache.json');
const CACHE_TTL_MS = Number(process.env.NEWS_CACHE_MS || 10 * 60 * 1000);
const GEMINI_MODEL = process.env.GEMINI_MODEL || 'gemini-2.0-flash';
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || '';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '123456';
const ADMIN_SESSION_TTL_MS = 12 * 60 * 60 * 1000;
const AUTO_REFRESH_TIMES = String(process.env.AUTO_REFRESH_TIMES || '08:00,20:00');
const MAX_NEW_SUMMARIES_PER_RUN = Number(process.env.MAX_NEW_SUMMARIES_PER_RUN || 12);
const USE_GEMINI_SUMMARY = String(process.env.USE_GEMINI_SUMMARY || 'false').toLowerCase() === 'true';
const PORT_FALLBACKS = String(process.env.PORT_FALLBACKS || '3130,3131,3132,3133,3134,3135')
  .split(',')
  .map((value) => Number(value.trim()))
  .filter((value) => Number.isInteger(value) && value > 0);

const CATEGORIES = ['war', 'local', 'economy', 'legal', 'ai', 'tech', 'aihub'];
const AUTO_NEWS_CATEGORIES = ['war', 'local', 'economy', 'ai', 'tech'];
const NEWS_SCOPES = ['global', 'local'];
const AIHUB_SECTIONS = ['announcements', 'tools', 'community', 'tutorials'];
const MAX_BODY_BYTES = 4 * 1024 * 1024;

const cache = {
  updatedAt: 0,
  newsAuto: null,
  source: 'seed',
  error: null,
  inFlight: null,
  nextRunAt: null
};

const sessions = new Map();
const summaryStore = {
  loaded: false,
  dirty: false,
  items: {}
};

const ALLOWED_STATIC = new Set([
  '/index.html',
  '/logo.png',
  '/Gemini_Generated_Image_5rtz8m5rtz8m5rtz.png'
]);

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.webp': 'image/webp',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.txt': 'text/plain; charset=utf-8'
};

const TRUSTED_DOMAINS = [
  'reuters.com',
  'apnews.com',
  'bbc.com',
  'cnn.com',
  'nytimes.com',
  'bloomberg.com',
  'wsj.com',
  'ft.com',
  'theguardian.com',
  'aljazeera.com',
  'skynewsarabia.com',
  'alarabiya.net',
  'youm7.com',
  'aawsat.com',
  'shorouknews.com',
  'scotusblog.com',
  'judiciary.uk',
  'openai.com',
  'blog.google'
];

const RSS_FEEDS = {
  war: [
    'https://feeds.bbci.co.uk/news/world/rss.xml',
    'https://rss.nytimes.com/services/xml/rss/nyt/World.xml',
    'https://www.aljazeera.com/xml/rss/all.xml'
  ],
  local: [
    'https://feeds.bbci.co.uk/news/world/middle_east/rss.xml',
    'https://www.aljazeera.com/xml/rss/all.xml'
  ],
  economy: [
    'https://feeds.bbci.co.uk/news/business/rss.xml',
    'https://rss.nytimes.com/services/xml/rss/nyt/Business.xml',
    'https://rss.nytimes.com/services/xml/rss/nyt/Economy.xml'
  ],
  legal: [
    'https://www.scotusblog.com/feed/',
    'https://www.judiciary.uk/feed/'
  ],
  ai: [
    'https://blog.google/technology/ai/rss/',
    'https://openai.com/news/rss.xml'
  ],
  tech: [
    'https://feeds.bbci.co.uk/news/technology/rss.xml',
    'https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml'
  ]
};

const BASE_LOCAL_ARABIC_FEEDS = ['https://feeds.bbci.co.uk/arabic/rss.xml'];
const LOCAL_NEWS_QUERIES = {
  war: ['الأخبار الجيوسياسية', 'مصر السياسة الخارجية'],
  local: ['أخبار مصر', 'مصر الآن'],
  economy: ['اقتصاد مصر', 'أسعار الفائدة مصر'],
  ai: ['ذكاء اصطناعي بالعربية', 'مصر الذكاء الاصطناعي'],
  tech: ['تكنولوجيا مصر', 'الأمن السيبراني العربي']
};

const GLOBAL_NEWS_QUERIES = {
  war: ['geopolitics world latest', 'international conflict latest'],
  local: ['middle east regional news', 'north africa latest news'],
  economy: ['global macro economy latest', 'interest rates inflation latest'],
  ai: ['artificial intelligence latest model release'],
  tech: ['technology cybersecurity latest global']
};

const GLOBAL_SPECIALIZED_FEEDS = {
  ai: [
    'https://openai.com/news/rss.xml',
    'https://blog.google/technology/ai/rss/'
  ],
  tech: [
    'https://feeds.bbci.co.uk/news/technology/rss.xml',
    'https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml'
  ]
};

const CATEGORY_LABELS_AR = {
  war: 'الجيوسياسية',
  local: 'محلي',
  economy: 'الاقتصاد',
  legal: 'القانون',
  ai: 'الذكاء الاصطناعي',
  tech: 'التقنية'
};

const LOCAL_CATEGORY_KEYWORDS = {
  war: ['حرب', 'صراع', 'عسكري', 'غزة', 'أوكرانيا', 'هدنة', 'هجوم', 'نزاع', 'إيران', 'إسرائيل'],
  local: ['مصر', 'القاهرة', 'مصري', 'الحكومة', 'محلي', 'وزارة', 'البرلمان', 'محافظ'],
  economy: ['اقتصاد', 'تضخم', 'فائدة', 'بنك', 'مالية', 'أسواق', 'نفط', 'سندات', 'عملة', 'صندوق'],
  legal: ['قانون', 'محكمة', 'قضية', 'قضائي', 'حكم', 'تشريع', 'دستور', 'نيابة', 'عدالة'],
  ai: ['ذكاء', 'اصطناعي', 'نموذج', 'تعلم', 'روبوت', 'Gemini', 'OpenAI', 'AI'],
  tech: ['تقنية', 'تكنولوجيا', 'سيبراني', 'اختراق', 'هاتف', 'برمجيات', 'رقمي', 'إنترنت']
};

const SEED_NEWS = {
  war: [
    {
      title: 'متابعة مباشرة للتطورات العسكرية العالمية',
      summary: 'موجز متابعة لحظية من مصدر رئيسي موثوق حتى وصول تغذية الأخبار المباشرة.',
      source: 'BBC World',
      source_url: 'https://www.bbc.com/news/world',
      published_at: '',
      image_url: '',
      category: 'war',
      origin: 'seed',
      verified: true
    },
    {
      title: 'تغطية عاجلة لأخبار النزاعات الدولية',
      summary: 'ملف متابعة مستمر للأحداث الأمنية والجيوسياسية من وكالة معتمدة.',
      source: 'Reuters World',
      source_url: 'https://www.reuters.com/world/',
      published_at: '',
      image_url: '',
      category: 'war',
      origin: 'seed',
      verified: true
    },
    {
      title: 'ملف ميداني: تحركات وتحليلات الموقف',
      summary: 'تحديثات تحليلية وفق بيانات رسمية وتقارير دولية قابلة للتحقق.',
      source: 'Al Jazeera',
      source_url: 'https://www.aljazeera.com/news/',
      published_at: '',
      image_url: '',
      category: 'war',
      origin: 'seed',
      verified: true
    }
  ],
  local: [
    {
      title: 'متابعة محلية: مصر والشأن الداخلي',
      summary: 'تحديثات محلية موثقة حول الاقتصاد المحلي والسياسات العامة والتطورات الداخلية.',
      source: 'BBC Middle East',
      source_url: 'https://www.bbc.com/news/world/middle_east',
      published_at: '',
      image_url: '',
      category: 'local',
      origin: 'seed',
      verified: true
    },
    {
      title: 'قراءة يومية للتطورات المحلية في مصر',
      summary: 'رصد أهم المستجدات ذات التأثير العام على المواطن والأسواق المحلية.',
      source: 'Reuters',
      source_url: 'https://www.reuters.com/world/africa/',
      published_at: '',
      image_url: '',
      category: 'local',
      origin: 'seed',
      verified: true
    },
    {
      title: 'ملف محلي: مؤسسات الدولة والخدمات',
      summary: 'تغطية متوازنة لأخبار الخدمات العامة والقرارات التنظيمية داخل مصر.',
      source: 'Sky News Arabia',
      source_url: 'https://www.skynewsarabia.com/middle-east',
      published_at: '',
      image_url: '',
      category: 'local',
      origin: 'seed',
      verified: true
    }
  ],
  economy: [
    {
      title: 'رصد مباشر للأسواق العالمية',
      summary: 'متابعة الأسواق، الفائدة، الطاقة، والتضخم من مصادر اقتصادية رئيسية.',
      source: 'Reuters Business',
      source_url: 'https://www.reuters.com/business/',
      published_at: '',
      image_url: '',
      category: 'economy',
      origin: 'seed',
      verified: true
    },
    {
      title: 'تحليل اتجاهات الاقتصاد الدولي',
      summary: 'مؤشرات الاقتصاد الكلي وأثرها على الاستثمار والأسواق الناشئة.',
      source: 'Financial Times',
      source_url: 'https://www.ft.com/world?format=rss',
      published_at: '',
      image_url: '',
      category: 'economy',
      origin: 'seed',
      verified: true
    },
    {
      title: 'تحديثات الشركات والقطاعات',
      summary: 'مستجدات نتائج الأعمال والتحركات القطاعية من منصات مالية معروفة.',
      source: 'Bloomberg',
      source_url: 'https://www.bloomberg.com/markets',
      published_at: '',
      image_url: '',
      category: 'economy',
      origin: 'seed',
      verified: true
    }
  ],
  legal: [
    {
      title: 'متابعة أحدث الأحكام والاتجاهات القضائية',
      summary: 'تجميع لأهم التطورات القانونية من منصات رسمية ومتخصصة.',
      source: 'SCOTUSblog',
      source_url: 'https://www.scotusblog.com/',
      published_at: '',
      image_url: '',
      category: 'legal',
      origin: 'seed',
      verified: true
    },
    {
      title: 'مستجدات التشريعات والإجراءات',
      summary: 'تغطية مستمرة للتعديلات النظامية وتأثيرها العملي على التقاضي.',
      source: 'Judiciary UK',
      source_url: 'https://www.judiciary.uk/news-and-media/',
      published_at: '',
      image_url: '',
      category: 'legal',
      origin: 'seed',
      verified: true
    },
    {
      title: 'رصد تحليلي للقرارات القانونية المؤثرة',
      summary: 'موجز مهني يربط الأحداث القانونية بسياقها التنظيمي والقضائي.',
      source: 'BBC',
      source_url: 'https://www.bbc.com/news',
      published_at: '',
      image_url: '',
      category: 'legal',
      origin: 'seed',
      verified: true
    }
  ],
  ai: [
    {
      title: 'أحدث إعلانات الذكاء الاصطناعي',
      summary: 'متابعة إطلاقات النماذج، الأدوات، وسياسات الاستخدام من المصادر الرسمية.',
      source: 'Google AI Blog',
      source_url: 'https://blog.google/technology/ai/',
      published_at: '',
      image_url: '',
      category: 'ai',
      origin: 'seed',
      verified: true
    },
    {
      title: 'تحديثات المنتجات والنماذج التوليدية',
      summary: 'أخبار القدرات الجديدة وتطبيقاتها العملية في العمل والقانون.',
      source: 'OpenAI News',
      source_url: 'https://openai.com/news/',
      published_at: '',
      image_url: '',
      category: 'ai',
      origin: 'seed',
      verified: true
    },
    {
      title: 'موجز أسبوعي للتطورات التقنية في AI',
      summary: 'أبرز ما صدر من تحديثات موثقة في الذكاء الاصطناعي وتكاملاته.',
      source: 'BBC Technology',
      source_url: 'https://www.bbc.com/news/technology',
      published_at: '',
      image_url: '',
      category: 'ai',
      origin: 'seed',
      verified: true
    }
  ],
  tech: [
    {
      title: 'متابعة أخبار التقنية والأمن السيبراني',
      summary: 'أخبار الاختراقات، التحديثات الأمنية، والبنية التحتية الرقمية.',
      source: 'BBC Tech',
      source_url: 'https://www.bbc.com/news/technology',
      published_at: '',
      image_url: '',
      category: 'tech',
      origin: 'seed',
      verified: true
    },
    {
      title: 'تطورات عالم الأجهزة والمنصات',
      summary: 'تغطية مستمرة لإطلاقات المنتجات الكبرى والتحولات التقنية.',
      source: 'NYTimes Technology',
      source_url: 'https://www.nytimes.com/section/technology',
      published_at: '',
      image_url: '',
      category: 'tech',
      origin: 'seed',
      verified: true
    },
    {
      title: 'مؤشرات أمن المعلومات اليومي',
      summary: 'رصد المخاطر السيبرانية والردود الأمنية من مصادر إعلامية موثوقة.',
      source: 'Reuters Technology',
      source_url: 'https://www.reuters.com/technology/',
      published_at: '',
      image_url: '',
      category: 'tech',
      origin: 'seed',
      verified: true
    }
  ],
  aihub: [
    {
      title: 'تحديثات AI HUB جاهزة لاستقبال مشاركات الأدمن',
      summary: 'يمكنك الآن إضافة أخبار يدوية بصور وروابط مصادر مباشرة من لوحة الأدمن.',
      source: 'AI HUB System',
      source_url: 'https://openai.com',
      published_at: '',
      image_url: '',
      category: 'aihub',
      aihub_section: 'announcements',
      origin: 'seed',
      verified: true
    },
    {
      title: 'قسم الأدوات والملفات قيد التشغيل',
      summary: 'أضف أي أداة أو ملف جديد مع رابط المصدر وسيظهر فورًا في تبويب AI HUB.',
      source: 'AI HUB System',
      source_url: 'https://openai.com',
      published_at: '',
      image_url: '',
      category: 'aihub',
      aihub_section: 'tools',
      origin: 'seed',
      verified: true
    },
    {
      title: 'قسم المجتمع والفعاليات مفعّل',
      summary: 'يمكنك مشاركة أخبار الورش، المواعيد، وإعلانات الجروب مع صورة لكل خبر.',
      source: 'AI HUB System',
      source_url: 'https://openai.com',
      published_at: '',
      image_url: '',
      category: 'aihub',
      aihub_section: 'community',
      origin: 'seed',
      verified: true
    },
    {
      title: 'قسم الشروحات جاهز للإضافات اليدوية',
      summary: 'أضف شروحات ودروس AI HUB من لوحة الأدمن فقط، وستظهر مباشرة داخل تبويب الشروحات.',
      source: 'AI HUB System',
      source_url: 'https://openai.com',
      published_at: '',
      image_url: '',
      category: 'aihub',
      aihub_section: 'tutorials',
      origin: 'seed',
      verified: true
    }
  ]
};

function ensureDataStore() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
  if (!fs.existsSync(MANUAL_NEWS_FILE)) {
    fs.writeFileSync(MANUAL_NEWS_FILE, JSON.stringify({ items: [] }, null, 2), 'utf8');
  }
  if (!fs.existsSync(SUMMARY_CACHE_FILE)) {
    fs.writeFileSync(SUMMARY_CACHE_FILE, JSON.stringify({ items: {} }, null, 2), 'utf8');
  }
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store'
  });
  res.end(JSON.stringify(payload));
}

function sendText(res, statusCode, text) {
  res.writeHead(statusCode, {
    'Content-Type': 'text/plain; charset=utf-8',
    'Cache-Control': 'no-store'
  });
  res.end(text);
}

function cleanupSessions() {
  const now = Date.now();
  for (const [token, expiresAt] of sessions.entries()) {
    if (expiresAt <= now) sessions.delete(token);
  }
}

function createSessionToken() {
  cleanupSessions();
  const token = crypto.randomBytes(24).toString('hex');
  sessions.set(token, Date.now() + ADMIN_SESSION_TTL_MS);
  return token;
}

function getAuthToken(req) {
  const auth = req.headers.authorization || '';
  if (auth.startsWith('Bearer ')) {
    return auth.slice(7).trim();
  }
  return String(req.headers['x-admin-token'] || '').trim();
}

function isAuthorized(req) {
  cleanupSessions();
  const token = getAuthToken(req);
  if (!token) return false;
  const expiresAt = sessions.get(token);
  if (!expiresAt || expiresAt <= Date.now()) {
    sessions.delete(token);
    return false;
  }
  return true;
}

function safeCompare(a, b) {
  const aBuf = Buffer.from(String(a));
  const bBuf = Buffer.from(String(b));
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    let bytes = 0;

    req.on('data', (chunk) => {
      bytes += chunk.length;
      if (bytes > MAX_BODY_BYTES) {
        reject(new Error('Payload too large'));
        req.destroy();
        return;
      }
      body += chunk;
    });

    req.on('end', () => {
      if (!body.trim()) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(body));
      } catch {
        reject(new Error('Invalid JSON body'));
      }
    });

    req.on('error', reject);
  });
}

function readManualNews() {
  try {
    const raw = fs.readFileSync(MANUAL_NEWS_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed.items)) return [];
    return parsed.items;
  } catch {
    return [];
  }
}

function writeManualNews(items) {
  fs.writeFileSync(MANUAL_NEWS_FILE, JSON.stringify({ items }, null, 2), 'utf8');
}

function ensureSummaryStoreLoaded() {
  if (summaryStore.loaded) return;
  try {
    const raw = fs.readFileSync(SUMMARY_CACHE_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    summaryStore.items = parsed && typeof parsed.items === 'object' && parsed.items ? parsed.items : {};
  } catch {
    summaryStore.items = {};
  }
  summaryStore.loaded = true;
}

function saveSummaryStore() {
  if (!summaryStore.loaded || !summaryStore.dirty) return;
  fs.writeFileSync(
    SUMMARY_CACHE_FILE,
    JSON.stringify({ items: summaryStore.items }, null, 2),
    'utf8'
  );
  summaryStore.dirty = false;
}

function stripHtml(input) {
  return String(input || '')
    .replace(/<[^>]*>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function decodeXmlEntities(input) {
  return String(input || '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, ' ');
}

function excerpt(input, maxLen = 220) {
  const clean = stripHtml(decodeXmlEntities(input));
  if (clean.length <= maxLen) return clean;
  return `${clean.slice(0, maxLen - 1).trim()}…`;
}

function looksArabicText(input) {
  return /[\u0600-\u06FF]/.test(String(input || ''));
}

function normalizeHttpUrl(value) {
  if (!value) return '';
  try {
    const url = new URL(String(value).trim());
    if (url.protocol !== 'http:' && url.protocol !== 'https:') return '';
    return url.toString();
  } catch {
    return '';
  }
}

function normalizeImageUrl(value) {
  if (!value) return '';
  const text = String(value).trim();
  if (text.startsWith('data:image/')) return text;
  return normalizeHttpUrl(text);
}

function sourceDomain(urlValue) {
  try {
    return new URL(urlValue).hostname.toLowerCase();
  } catch {
    return '';
  }
}

function isTrustedSourceUrl(urlValue) {
  const host = sourceDomain(urlValue);
  if (!host) return false;
  return TRUSTED_DOMAINS.some((domain) => host === domain || host.endsWith(`.${domain}`));
}

function normalizeScope(rawScope, category, title, summary, sourceUrl) {
  const candidate = String(rawScope || '').trim().toLowerCase();
  if (NEWS_SCOPES.includes(candidate)) return candidate;
  if (category === 'local') return 'local';

  const host = sourceDomain(sourceUrl);
  const text = `${String(title || '')} ${String(summary || '')}`;
  const isArabic = looksArabicText(text);
  if (host.endsWith('.eg') || host.includes('arab') || isArabic) return 'local';
  return 'global';
}

function matchesCategoryKeyword(category, text) {
  const keywords = LOCAL_CATEGORY_KEYWORDS[category] || [];
  if (!keywords.length) return true;
  const haystack = String(text || '').toLowerCase();
  return keywords.some((keyword) => haystack.includes(String(keyword).toLowerCase()));
}

function buildGoogleNewsRssQuery(query, language = 'ar', country = 'EG') {
  const q = encodeURIComponent(String(query || '').trim());
  if (!q) return '';
  const lang = String(language || 'ar').toLowerCase();
  const ctry = String(country || 'EG').toUpperCase();
  return `https://news.google.com/rss/search?q=${q}&hl=${lang}&gl=${ctry}&ceid=${ctry}:${lang}`;
}

function getFeedsForCategoryScope(category, scope) {
  if (scope === 'local') {
    const queries = LOCAL_NEWS_QUERIES[category] || LOCAL_NEWS_QUERIES.local;
    const queryFeeds = queries.map((query) => buildGoogleNewsRssQuery(query, 'ar', 'EG')).filter(Boolean);
    return dedupeByKey(
      [...BASE_LOCAL_ARABIC_FEEDS, ...queryFeeds].map((url) => ({
        source_url: normalizeHttpUrl(url),
        title: url
      }))
    )
      .map((item) => item.source_url)
      .filter(Boolean);
  }

  const queries = GLOBAL_NEWS_QUERIES[category] || [];
  const queryFeeds = queries.map((query) => buildGoogleNewsRssQuery(query, 'en', 'US')).filter(Boolean);
  const directFeeds = [
    ...(RSS_FEEDS[category] || []),
    ...(GLOBAL_SPECIALIZED_FEEDS[category] || [])
  ];
  return dedupeByKey(
    [...directFeeds, ...queryFeeds].map((url) => ({
      source_url: normalizeHttpUrl(url),
      title: url
    }))
  )
    .map((item) => item.source_url)
    .filter(Boolean);
}

function dedupeByKey(items) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const key = (item.source_url || item.title || '').toLowerCase().trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

function toTimestamp(value) {
  if (!value) return 0;
  const ts = Date.parse(String(value));
  return Number.isFinite(ts) ? ts : 0;
}

function sortNewsItemsByFreshness(items) {
  return [...(Array.isArray(items) ? items : [])].sort((a, b) => {
    const bTs = Math.max(
      toTimestamp(b?.created_at),
      toTimestamp(b?.published_at),
      toTimestamp(b?.updated_at)
    );
    const aTs = Math.max(
      toTimestamp(a?.created_at),
      toTimestamp(a?.published_at),
      toTimestamp(a?.updated_at)
    );
    return bTs - aTs;
  });
}

function ensureNewsShape(news) {
  const shaped = {};
  for (const category of CATEGORIES) {
    shaped[category] = Array.isArray(news?.[category]) ? news[category] : [];
  }
  return shaped;
}

function parseRssItems(xmlText) {
  const xml = String(xmlText || '');
  const itemMatches = [...xml.matchAll(/<item\b[\s\S]*?<\/item>/gi)].map((m) => m[0]);
  const entryMatches = itemMatches.length
    ? []
    : [...xml.matchAll(/<entry\b[\s\S]*?<\/entry>/gi)].map((m) => m[0]);
  const blocks = itemMatches.length ? itemMatches : entryMatches;

  const extractTag = (block, tag) => {
    const match = block.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i'));
    return match ? decodeXmlEntities(match[1]) : '';
  };

  const extractLink = (block) => {
    const hrefMatch = block.match(/<link[^>]*href=["']([^"']+)["'][^>]*>/i);
    if (hrefMatch) return decodeXmlEntities(hrefMatch[1]);

    const textMatch = block.match(/<link[^>]*>([\s\S]*?)<\/link>/i);
    return textMatch ? decodeXmlEntities(textMatch[1]) : '';
  };

  const extractImage = (block) => {
    const media = block.match(/<media:content[^>]*url=["']([^"']+)["'][^>]*>/i);
    if (media) return decodeXmlEntities(media[1]);

    const enclosure = block.match(/<enclosure[^>]*url=["']([^"']+)["'][^>]*>/i);
    if (enclosure) return decodeXmlEntities(enclosure[1]);

    const img = block.match(/<img[^>]*src=["']([^"']+)["'][^>]*>/i);
    return img ? decodeXmlEntities(img[1]) : '';
  };

  return blocks.map((block) => ({
    title: extractTag(block, 'title'),
    description: extractTag(block, 'description') || extractTag(block, 'summary') || extractTag(block, 'content'),
    link: extractLink(block),
    pubDate: extractTag(block, 'pubDate') || extractTag(block, 'updated') || extractTag(block, 'published'),
    image: extractImage(block)
  }));
}

async function fetchText(url, timeoutMs = 10000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        'User-Agent': 'AI-HUB-NewsBot/1.0 (+https://openai.com)',
        Accept: 'text/html,application/rss+xml,application/xml;q=0.9,*/*;q=0.8'
      }
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.text();
  } finally {
    clearTimeout(timer);
  }
}

function normalizedAutoItem(raw, category, origin) {
  const sourceUrl = normalizeHttpUrl(raw.source_url || raw.link || '');
  if (!sourceUrl) return null;

  const title = String(raw.title || '').trim();
  const summary = excerpt(raw.summary || raw.description || '', 240);
  const source = String(raw.source || '').trim() || sourceDomain(sourceUrl);
  const publishedAt = String(raw.published_at || raw.pubDate || '').trim();
  const imageUrl = normalizeImageUrl(raw.image_url || raw.image || '');

  if (!title || title.length < 6) return null;
  if (!summary || summary.length < 20) return null;

  return {
    id: `auto_${crypto.randomBytes(6).toString('hex')}`,
    category,
    scope: normalizeScope(raw.scope, category, title, summary, sourceUrl),
    aihub_section: category === 'aihub' ? String(raw.aihub_section || 'announcements') : '',
    title,
    summary,
    source,
    source_url: sourceUrl,
    image_url: imageUrl,
    published_at: publishedAt,
    origin,
    verified: isTrustedSourceUrl(sourceUrl)
  };
}

async function fetchCategoryFromRss(category, scope = 'global') {
  const feeds = getFeedsForCategoryScope(category, scope);
  const collected = [];

  for (const feedUrl of feeds) {
    try {
      const xml = await fetchText(feedUrl, 9000);
      const parsed = parseRssItems(xml)
        .map((item) => {
          const sourceUrl = normalizeHttpUrl(item.link);
          const sourceName = sourceDomain(sourceUrl).replace(/^www\./, '');
          const contentText = `${item.title || ''} ${item.description || ''}`;
          if (scope === 'local') {
            if (!looksArabicText(contentText)) return null;
            if (!matchesCategoryKeyword(category, contentText)) return null;
          }
          if (scope === 'global') {
            // Keep the global feeds primarily English to reduce mixing and speed screening.
            if (looksArabicText(contentText)) return null;
          }
          return normalizedAutoItem(
            {
              title: item.title,
              summary: item.description,
              source: sourceName,
              source_url: sourceUrl,
              published_at: item.pubDate,
              image_url: item.image,
              scope
            },
            category,
            'rss'
          );
        })
        .filter(Boolean)
        .filter((item) => item.verified)
        .slice(0, scope === 'local' ? 10 : 8);

      collected.push(...parsed);
      if (collected.length >= 24) break;
    } catch {
      // Ignore single feed failures and continue.
    }
  }

  return sortNewsItemsByFreshness(dedupeByKey(collected)).slice(0, 24);
}

function stripFences(raw) {
  if (!raw) return '';
  return raw.replace(/^\s*```(?:json)?/i, '').replace(/```\s*$/i, '').trim();
}

function setCachedSummary(urlValue, summary, by = 'fallback') {
  if (!urlValue || !summary) return;
  ensureSummaryStoreLoaded();
  summaryStore.items[urlValue] = {
    summary: excerpt(summary, 240),
    by,
    updated_at: new Date().toISOString()
  };

  const keys = Object.keys(summaryStore.items);
  if (keys.length > 8000) {
    const sorted = keys
      .map((key) => ({ key, ts: Date.parse(summaryStore.items[key]?.updated_at || 0) || 0 }))
      .sort((a, b) => b.ts - a.ts);
    const keep = new Set(sorted.slice(0, 8000).map((row) => row.key));
    for (const key of keys) {
      if (!keep.has(key)) delete summaryStore.items[key];
    }
  }

  summaryStore.dirty = true;
}

function getCachedSummary(urlValue) {
  if (!urlValue) return '';
  ensureSummaryStoreLoaded();
  const entry = summaryStore.items[urlValue];
  if (!entry || typeof entry.summary !== 'string') return '';
  return entry.summary.trim();
}

function parseSummaryArray(rawValue) {
  if (Array.isArray(rawValue)) return rawValue;
  if (rawValue && Array.isArray(rawValue.items)) return rawValue.items;
  if (rawValue && Array.isArray(rawValue.summaries)) return rawValue.summaries;
  return [];
}

async function summarizeBatchWithGemini(batch) {
  if (!GEMINI_API_KEY || !Array.isArray(batch) || !batch.length) return new Map();

  const promptLines = [
    'لخّص الأخبار التالية في العربية الفصحى بدقة عالية.',
    'أعد JSON فقط بدون شرح.',
    'الشكل:',
    '{"items":[{"source_url":"","summary":""}]}',
    'قواعد:',
    '1) summary من 40 إلى 220 حرفًا.',
    '2) لا تخترع معلومات خارج النص.',
    '3) حافظ على المعلومة الرئيسية والسياق.',
    'الأخبار:'
  ];

  batch.forEach((item, idx) => {
    promptLines.push(
      `${idx + 1}) source_url=${item.source_url}\n` +
      `title=${item.title || ''}\n` +
      `text=${excerpt(item.summary || '', 600)}`
    );
  });

  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    GEMINI_MODEL
  )}:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;

  const body = {
    contents: [{ role: 'user', parts: [{ text: promptLines.join('\n\n') }] }],
    generationConfig: {
      temperature: 0.1,
      responseMimeType: 'application/json'
    }
  };

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`Gemini summarize ${response.status}: ${errText.slice(0, 220)}`);
  }

  const payload = await response.json();
  const outputText = payload?.candidates?.[0]?.content?.parts?.map((part) => part?.text || '').join('\n').trim();
  if (!outputText) return new Map();

  const parsed = JSON.parse(stripFences(outputText));
  const rows = parseSummaryArray(parsed);
  const out = new Map();
  for (const row of rows) {
    const sourceUrl = normalizeHttpUrl(row?.source_url || '');
    const summary = excerpt(String(row?.summary || '').trim(), 240);
    if (!sourceUrl || summary.length < 20) continue;
    out.set(sourceUrl, summary);
  }
  return out;
}

async function enrichSummariesFromCache(items) {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return { cache_hits: 0, summarized_now: 0 };

  ensureSummaryStoreLoaded();
  const pendingByUrl = new Map();
  let cacheHits = 0;
  let summarizedNow = 0;

  for (const item of list) {
    const sourceUrl = normalizeHttpUrl(item?.source_url || '');
    if (!sourceUrl) continue;
    const cached = getCachedSummary(sourceUrl);
    if (cached) {
      item.summary = cached;
      cacheHits += 1;
      continue;
    }

    if (!pendingByUrl.has(sourceUrl)) pendingByUrl.set(sourceUrl, []);
    pendingByUrl.get(sourceUrl).push(item);
  }

  const pendingEntries = [...pendingByUrl.entries()];
  const limited = pendingEntries.slice(0, Math.max(0, MAX_NEW_SUMMARIES_PER_RUN));
  if (USE_GEMINI_SUMMARY && GEMINI_API_KEY) {
    const chunks = [];
    for (let i = 0; i < limited.length; i += 6) {
      chunks.push(limited.slice(i, i + 6));
    }

    for (const chunk of chunks) {
      try {
        const promptItems = chunk.map(([sourceUrl, scopedItems]) => ({
          source_url: sourceUrl,
          title: String(scopedItems[0]?.title || ''),
          summary: String(scopedItems[0]?.summary || '')
        }));
        const mapped = await summarizeBatchWithGemini(promptItems);
        for (const [sourceUrl, scopedItems] of chunk) {
          const aiSummary = mapped.get(sourceUrl);
          const summary = aiSummary || excerpt(scopedItems[0]?.summary || scopedItems[0]?.title || '', 240);
          scopedItems.forEach((item) => {
            item.summary = summary;
          });
          setCachedSummary(sourceUrl, summary, aiSummary ? GEMINI_MODEL : 'fallback');
          if (aiSummary) summarizedNow += scopedItems.length;
        }
      } catch {
        for (const [sourceUrl, scopedItems] of chunk) {
          const summary = excerpt(scopedItems[0]?.summary || scopedItems[0]?.title || '', 240);
          scopedItems.forEach((item) => {
            item.summary = summary;
          });
          setCachedSummary(sourceUrl, summary, 'fallback');
        }
      }
    }
  } else {
    for (const [sourceUrl, scopedItems] of limited) {
      const summary = excerpt(scopedItems[0]?.summary || scopedItems[0]?.title || '', 240);
      scopedItems.forEach((item) => {
        item.summary = summary;
      });
      setCachedSummary(sourceUrl, summary, 'fallback');
    }
  }

  // Any items beyond MAX_NEW_SUMMARIES_PER_RUN are cached with deterministic fallback to avoid recurring API calls.
  for (const [sourceUrl, scopedItems] of pendingEntries.slice(limited.length)) {
    const summary = excerpt(scopedItems[0]?.summary || scopedItems[0]?.title || '', 240);
    scopedItems.forEach((item) => {
      item.summary = summary;
    });
    setCachedSummary(sourceUrl, summary, 'fallback');
  }

  saveSummaryStore();
  return {
    cache_hits: cacheHits,
    summarized_now: summarizedNow
  };
}

function normalizeGeminiPayload(payload) {
  const shaped = {};

  for (const category of ['war', 'local', 'economy', 'legal', 'ai', 'tech']) {
    const rawItems = Array.isArray(payload?.[category]) ? payload[category] : [];
    shaped[category] = dedupeByKey(
      rawItems
        .map((item) =>
          normalizedAutoItem(
            {
              title: item?.title,
              summary: item?.summary,
              source: item?.source,
              source_url: item?.source_url,
              published_at: item?.published_at,
              image_url: item?.image_url
            },
            category,
            'gemini'
          )
        )
        .filter(Boolean)
        .filter((item) => item.verified)
    ).slice(0, 16);
  }

  shaped.aihub = [];
  return shaped;
}

async function fetchFromGemini() {
  if (!GEMINI_API_KEY) {
    throw new Error('GEMINI_API_KEY is missing');
  }

  const prompt = [
    'أنت نظام أخبار لحظي.',
    'أعد JSON فقط بدون أي شرح.',
    'أريد 3 أخبار على الأقل لكل قسم: war,local,economy,legal,ai,tech.',
    'لكل خبر أضف الحقول التالية إلزاميًا:',
    'title, summary, source, source_url, published_at, image_url',
    'قواعد صارمة:',
    '1) source_url يجب أن يكون رابط الخبر الرئيسي المباشر.',
    '2) summary من 40 إلى 240 حرفًا.',
    '3) لا تكرر نفس الخبر.',
    '4) اللغة العربية الفصحى.',
    'البنية:',
    '{',
    '  "war": [{"title":"","summary":"","source":"","source_url":"","published_at":"","image_url":""}],',
    '  "local": [...],',
    '  "economy": [...],',
    '  "legal": [...],',
    '  "ai": [...],',
    '  "tech": [...]',
    '}'
  ].join('\n');

  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    GEMINI_MODEL
  )}:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;

  const body = {
    contents: [{ role: 'user', parts: [{ text: prompt }] }],
    generationConfig: {
      temperature: 0.2,
      responseMimeType: 'application/json'
    },
    tools: [{ google_search: {} }]
  };

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });

  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`Gemini API ${response.status}: ${errText.slice(0, 250)}`);
  }

  const data = await response.json();
  const outputText = data?.candidates?.[0]?.content?.parts?.map((part) => part?.text || '').join('\n').trim();
  if (!outputText) {
    throw new Error('Gemini returned an empty payload');
  }

  const parsed = JSON.parse(stripFences(outputText));
  return normalizeGeminiPayload(parsed);
}

function withSeedIfNeeded(items, category, minCount = 3) {
  const result = Array.isArray(items) ? [...items] : [];
  const seeds = Array.isArray(SEED_NEWS[category]) ? SEED_NEWS[category] : [];
  let seedIndex = 0;

  while (result.length < minCount && seedIndex < seeds.length) {
    const seed = {
      ...seeds[seedIndex],
      id: `seed_${category}_${seedIndex + 1}`,
      published_at: seeds[seedIndex].published_at || ''
    };
    result.push(seed);
    seedIndex += 1;
  }

  return result;
}

function mergeCategoryItems(...groups) {
  const merged = [];
  for (const group of groups) {
    if (Array.isArray(group)) merged.push(...group);
  }
  return sortNewsItemsByFreshness(dedupeByKey(merged));
}

async function buildAutoNews() {
  const result = ensureNewsShape({});
  const warnings = [];
  const rssNews = {};
  for (const category of AUTO_NEWS_CATEGORIES) {
    const globalItems = await fetchCategoryFromRss(category, 'global');
    const localItems = await fetchCategoryFromRss(category, 'local');
    rssNews[category] = mergeCategoryItems(globalItems, localItems).slice(0, 30);
  }

  const allRssItems = AUTO_NEWS_CATEGORIES.flatMap((category) => rssNews[category] || []);
  try {
    await enrichSummariesFromCache(allRssItems);
  } catch (error) {
    warnings.push(`Summary cache fallback: ${error.message}`);
  }

  for (const category of AUTO_NEWS_CATEGORIES) {
    result[category] = withSeedIfNeeded(rssNews[category] || [], category, 3);
  }

  result.legal = [];
  result.aihub = withSeedIfNeeded([], 'aihub', 3);

  return {
    news: result,
    source: USE_GEMINI_SUMMARY && GEMINI_API_KEY
      ? `rss+summary-cache:${GEMINI_MODEL}`
      : 'rss+summary-cache:cache-only',
    warnings
  };
}

function escapeRegExp(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function extractHtmlTitle(html) {
  const match = String(html || '').match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return match ? excerpt(match[1], 180) : '';
}

function extractMetaValue(html, keys, maxLen = 280) {
  const source = String(html || '');
  for (const key of keys) {
    const k = escapeRegExp(key);
    const patterns = [
      new RegExp(`<meta[^>]*(?:name|property|itemprop)=["']${k}["'][^>]*content=["']([^"']+)["'][^>]*>`, 'i'),
      new RegExp(`<meta[^>]*content=["']([^"']+)["'][^>]*(?:name|property|itemprop)=["']${k}["'][^>]*>`, 'i')
    ];
    for (const pattern of patterns) {
      const match = source.match(pattern);
      if (match?.[1]) {
        const clean = stripHtml(decodeXmlEntities(match[1])).trim();
        if (!clean) continue;
        return maxLen > 0 ? excerpt(clean, maxLen) : clean;
      }
    }
  }
  return '';
}

function extractFirstParagraph(html) {
  const match = String(html || '').match(/<p\b[^>]*>([\s\S]*?)<\/p>/i);
  return match ? excerpt(match[1], 280) : '';
}

function extractArticleTextSnippet(html) {
  const clean = String(html || '')
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<[^>]*>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return excerpt(clean, 320);
}

function titleFromUrlPath(sourceUrl) {
  try {
    const url = new URL(sourceUrl);
    const segments = url.pathname.split('/').filter(Boolean);
    const last = segments[segments.length - 1] || '';
    if (!last) return '';
    const normalized = decodeURIComponent(last)
      .replace(/[-_]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    return excerpt(normalized, 120);
  } catch {
    return '';
  }
}

function fallbackArticleFromUrl(sourceUrl, category) {
  const source = sourceDomain(sourceUrl).replace(/^www\./, '') || 'trusted-source';
  const pathTitle = titleFromUrlPath(sourceUrl);
  const fallbackTitle = pathTitle && pathTitle.length >= 6 ? pathTitle : `خبر جديد من ${source}`;
  const fallbackSummary = `ملخص أولي من الرابط المضاف يدويًا. يرجى مراجعة التفاصيل من المصدر الرئيسي: ${source}.`;
  return {
    title: fallbackTitle,
    summary: fallbackSummary,
    source,
    image_url: '',
    published_at: '',
    scope: normalizeScope('', category, fallbackTitle, fallbackSummary, sourceUrl)
  };
}

async function extractArticleFromUrl(sourceUrl, category) {
  const html = await fetchText(sourceUrl, 12000);
  const title =
    extractMetaValue(html, ['og:title', 'twitter:title']) ||
    extractHtmlTitle(html);
  const summary =
    extractMetaValue(html, ['og:description', 'description', 'twitter:description']) ||
    extractFirstParagraph(html) ||
    extractArticleTextSnippet(html);
  const imageUrl = normalizeImageUrl(
    extractMetaValue(html, ['og:image', 'twitter:image'], 0)
  );
  const publishedAt =
    extractMetaValue(html, ['article:published_time', 'og:updated_time', 'datePublished'], 0) ||
    '';
  const source = sourceDomain(sourceUrl).replace(/^www\./, '');

  return {
    title: String(title || '').trim(),
    summary: String(summary || '').trim(),
    source: String(source || '').trim(),
    image_url: imageUrl,
    published_at: publishedAt,
    scope: normalizeScope('', category, title, summary, sourceUrl)
  };
}

async function normalizeManualItem(raw) {
  const category = CATEGORIES.includes(raw?.category) ? raw.category : '';
  if (!category) {
    throw new Error('Category is required and must be valid');
  }

  const sourceUrl = normalizeHttpUrl(raw?.source_url || '');
  if (!sourceUrl) throw new Error('Valid source_url is required');

  let title = String(raw?.title || '').trim();
  let summary = String(raw?.summary || '').trim();
  let source = String(raw?.source || '').trim();
  let imageUrl = normalizeImageUrl(raw?.image_url || '');
  let publishedAt = String(raw?.published_at || '').trim() || new Date().toISOString();
  let extractedScope = '';

  const shouldExtract = Boolean(raw?.auto_extract) || !title || !summary || !source || !imageUrl;
  if (shouldExtract) {
    try {
      const extracted = await extractArticleFromUrl(sourceUrl, category);
      if (!title) title = extracted.title;
      if (!summary) summary = extracted.summary;
      if (!source) source = extracted.source;
      if (!imageUrl) imageUrl = extracted.image_url;
      if (!String(raw?.published_at || '').trim()) {
        publishedAt = extracted.published_at || publishedAt;
      }
      extractedScope = extracted.scope || '';
      if (summary.length >= 20) {
        setCachedSummary(sourceUrl, summary, 'article-meta');
      }
    } catch (error) {
      if (!title || !summary || !source) {
        const fallback = fallbackArticleFromUrl(sourceUrl, category);
        if (!title) title = fallback.title;
        if (!summary) summary = fallback.summary;
        if (!source) source = fallback.source;
        if (!imageUrl) imageUrl = fallback.image_url;
        extractedScope = fallback.scope || extractedScope;
      }
    }
  }

  const cachedSummary = getCachedSummary(sourceUrl);
  if (!summary && cachedSummary) summary = cachedSummary;

  if (title.length < 6) throw new Error('Title must be at least 6 characters');
  if (summary.length < 20) throw new Error('Summary must be at least 20 characters');
  if (source.length < 2) throw new Error('Source name is required');

  let aihubSection = '';
  if (category === 'aihub') {
    aihubSection = AIHUB_SECTIONS.includes(raw?.aihub_section) ? raw.aihub_section : 'announcements';
  }

  return {
    id: `manual_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`,
    category,
    scope: normalizeScope(raw?.scope || extractedScope, category, title, summary, sourceUrl),
    aihub_section: aihubSection,
    title,
    summary,
    source,
    source_url: sourceUrl,
    image_url: imageUrl,
    published_at: publishedAt,
    created_at: new Date().toISOString(),
    origin: 'manual',
    verified: isTrustedSourceUrl(sourceUrl)
  };
}

function mergeManualWithAuto(autoNews, manualItems) {
  const output = ensureNewsShape(autoNews);

  const manualByCategory = {};
  for (const category of CATEGORIES) {
    manualByCategory[category] = manualItems
      .filter((item) => item.category === category)
      .map((item) => ({
        ...item,
        scope: normalizeScope(item.scope, category, item.title, item.summary, item.source_url)
      }))
      .sort((a, b) => new Date(b.created_at || b.published_at || 0) - new Date(a.created_at || a.published_at || 0));
  }

  for (const category of CATEGORIES) {
    const merged = mergeCategoryItems(
      manualByCategory[category],
      (autoNews[category] || []).map((item) => ({
        ...item,
        scope: normalizeScope(item.scope, category, item.title, item.summary, item.source_url)
      }))
    );
    if (category === 'legal') {
      // Legal & judiciary tab is manual-only by user request.
      output[category] = sortNewsItemsByFreshness(manualByCategory[category]);
    } else {
      output[category] = withSeedIfNeeded(merged, category, 3);
    }
  }

  return output;
}

function splitAiHubSections(items) {
  const announcements = [];
  const tools = [];
  const community = [];
  const tutorials = [];

  for (const item of items) {
    if (item.aihub_section === 'tools') {
      tools.push(item);
    } else if (item.aihub_section === 'community') {
      community.push(item);
    } else if (item.aihub_section === 'tutorials') {
      tutorials.push(item);
    } else {
      announcements.push(item);
    }
  }

  const seedBySection = {
    announcements: (SEED_NEWS.aihub || []).filter((item) => item.aihub_section === 'announcements'),
    tools: (SEED_NEWS.aihub || []).filter((item) => item.aihub_section === 'tools'),
    community: (SEED_NEWS.aihub || []).filter((item) => item.aihub_section === 'community'),
    tutorials: (SEED_NEWS.aihub || []).filter((item) => item.aihub_section === 'tutorials')
  };

  if (!announcements.length && seedBySection.announcements.length) {
    announcements.push({ ...seedBySection.announcements[0], id: 'seed_aihub_announcements' });
  }
  if (!tools.length && seedBySection.tools.length) {
    tools.push({ ...seedBySection.tools[0], id: 'seed_aihub_tools' });
  }
  if (!community.length && seedBySection.community.length) {
    community.push({ ...seedBySection.community[0], id: 'seed_aihub_community' });
  }
  if (!tutorials.length && seedBySection.tutorials.length) {
    tutorials.push({ ...seedBySection.tutorials[0], id: 'seed_aihub_tutorials' });
  }

  return {
    announcements,
    tools,
    community,
    tutorials
  };
}

function scopeifySeedItem(item, category, scope, idSuffix) {
  return {
    ...item,
    id: `seed_${category}_${scope}_${idSuffix}`,
    category,
    scope,
    origin: 'seed',
    verified: true,
    published_at: item.published_at || ''
  };
}

function getScopedSeeds(category, scope) {
  if (scope === 'global') {
    const base = Array.isArray(SEED_NEWS[category]) ? SEED_NEWS[category] : [];
    return base.map((item, idx) => scopeifySeedItem(item, category, 'global', idx + 1));
  }

  if (category === 'local') {
    const baseLocal = Array.isArray(SEED_NEWS.local) ? SEED_NEWS.local : [];
    return baseLocal.map((item, idx) => scopeifySeedItem(item, 'local', 'local', idx + 1));
  }

  const localBase = Array.isArray(SEED_NEWS.local) ? SEED_NEWS.local : [];
  const categoryLabel = CATEGORY_LABELS_AR[category] || 'الأخبار';
  return localBase.map((item, idx) =>
    scopeifySeedItem(
      {
        ...item,
        title: `${categoryLabel} محليًا: ${item.title}`,
        category
      },
      category,
      'local',
      idx + 1
    )
  );
}

function withScopeSeedIfNeeded(items, category, scope, minCount = 3) {
  const normalized = (Array.isArray(items) ? items : [])
    .map((item) => ({
      ...item,
      scope: normalizeScope(item.scope, category, item.title, item.summary, item.source_url)
    }))
    .filter((item) => item.scope === scope);

  const out = [...normalized];
  const seeds = getScopedSeeds(category, scope);
  let idx = 0;
  while (out.length < minCount && idx < seeds.length) {
    out.push(seeds[idx]);
    idx += 1;
  }
  return out;
}

function buildNewsScopes(news) {
  const scoped = {};
  for (const category of AUTO_NEWS_CATEGORIES) {
    const items = Array.isArray(news?.[category]) ? news[category] : [];
    scoped[category] = {
      global: withScopeSeedIfNeeded(items, category, 'global', 3),
      local: withScopeSeedIfNeeded(items, category, 'local', 3)
    };
  }
  return scoped;
}

function parseAutoRefreshTimes() {
  const times = AUTO_REFRESH_TIMES.split(',')
    .map((value) => value.trim())
    .filter(Boolean)
    .map((value) => {
      const [h, m] = value.split(':').map((x) => Number(x));
      if (!Number.isInteger(h) || !Number.isInteger(m)) return null;
      if (h < 0 || h > 23 || m < 0 || m > 59) return null;
      return { hour: h, minute: m };
    })
    .filter(Boolean);

  if (!times.length) return [{ hour: 8, minute: 0 }, { hour: 20, minute: 0 }];
  return times;
}

function getNextRunAt(baseDate = new Date()) {
  const now = new Date(baseDate);
  const slots = parseAutoRefreshTimes();
  const candidates = slots.map((slot) => {
    const date = new Date(now);
    date.setHours(slot.hour, slot.minute, 0, 0);
    if (date <= now) {
      date.setDate(date.getDate() + 1);
    }
    return date;
  });
  candidates.sort((a, b) => a - b);
  return candidates[0];
}

function scheduleNextAutoRefresh() {
  const nextRun = getNextRunAt(new Date());
  cache.nextRunAt = nextRun.toISOString();
  const delay = Math.max(1000, nextRun.getTime() - Date.now());

  setTimeout(async () => {
    try {
      await refreshAutoNews(true);
    } catch {
      // Keep scheduler alive even if refresh fails.
    } finally {
      scheduleNextAutoRefresh();
    }
  }, delay);
}

async function refreshAutoNews(force = false) {
  if (!force && cache.newsAuto) return cache;
  if (cache.inFlight) return cache.inFlight;

  cache.inFlight = (async () => {
    try {
      const autoData = await buildAutoNews();
      cache.newsAuto = autoData.news;
      cache.updatedAt = Date.now();
      cache.source = autoData.source;
      cache.error = autoData.warnings.length ? autoData.warnings.join(' | ') : null;
      return cache;
    } catch (error) {
      cache.error = error.message;
      cache.source = 'seed';
      if (!cache.newsAuto) {
        cache.newsAuto = ensureNewsShape(SEED_NEWS);
      }
      if (!cache.updatedAt) {
        cache.updatedAt = Date.now();
      }
      return cache;
    } finally {
      cache.inFlight = null;
    }
  })();

  return cache.inFlight;
}

async function getMergedNews(force = false) {
  await refreshAutoNews(force);

  const manualItems = readManualNews();
  const mergedNews = mergeManualWithAuto(cache.newsAuto || ensureNewsShape(SEED_NEWS), manualItems);
  const newsScopes = buildNewsScopes(mergedNews);

  return {
    updatedAt: cache.updatedAt ? new Date(cache.updatedAt).toISOString() : new Date().toISOString(),
    next_run_at: cache.nextRunAt,
    source: cache.source,
    error: cache.error,
    news: mergedNews,
    news_scopes: newsScopes,
    aihub_sections: splitAiHubSections(mergedNews.aihub),
    stats: {
      min_required_per_auto_category: 3,
      counts: Object.fromEntries(CATEGORIES.map((category) => [category, mergedNews[category].length])),
      scoped_counts: Object.fromEntries(
        AUTO_NEWS_CATEGORIES.map((category) => [
          category,
          {
            global: newsScopes[category]?.global?.length || 0,
            local: newsScopes[category]?.local?.length || 0
          }
        ])
      ),
      manual_items_count: manualItems.length,
      summary_mode: USE_GEMINI_SUMMARY ? `gemini:${GEMINI_MODEL}` : 'cache-only',
      verified_policy: 'All auto items require trusted source domains and a source_url.'
    }
  };
}

function getAutoStatusPayload() {
  return {
    updated_at: cache.updatedAt ? new Date(cache.updatedAt).toISOString() : null,
    next_run_at: cache.nextRunAt,
    schedule_times: parseAutoRefreshTimes().map((slot) => `${String(slot.hour).padStart(2, '0')}:${String(slot.minute).padStart(2, '0')}`),
    source: cache.source,
    error: cache.error,
    summary_mode: USE_GEMINI_SUMMARY ? `gemini:${GEMINI_MODEL}` : 'cache-only'
  };
}

async function refreshBySourceCategory(categoryInput = 'all') {
  const allowed = [...AUTO_NEWS_CATEGORIES];
  const targetCategories = categoryInput === 'all' ? allowed : [categoryInput];
  const invalid = targetCategories.find((category) => !allowed.includes(category));
  if (invalid) {
    throw new Error(`Unsupported category: ${invalid}`);
  }

  if (!cache.newsAuto) {
    cache.newsAuto = ensureNewsShape(SEED_NEWS);
  }

  const updatedCounts = {};
  for (const category of targetCategories) {
    const sourceItems = mergeCategoryItems(
      await fetchCategoryFromRss(category, 'global'),
      await fetchCategoryFromRss(category, 'local')
    );
    await enrichSummariesFromCache(sourceItems);
    const merged = mergeCategoryItems(sourceItems, cache.newsAuto[category] || []);
    cache.newsAuto[category] = withSeedIfNeeded(merged, category, 3);
    updatedCounts[category] = cache.newsAuto[category].length;
  }

  cache.updatedAt = Date.now();
  cache.source = `rss-admin:${targetCategories.join(',')}`;
  return {
    updated_at: new Date(cache.updatedAt).toISOString(),
    source: cache.source,
    updated_counts: updatedCounts
  };
}

async function fetchYahooChartQuote(symbol) {
  const endpoint = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=5d&interval=1d`;
  const response = await fetch(endpoint);
  if (!response.ok) {
    throw new Error(`Yahoo chart API HTTP ${response.status} for ${symbol}`);
  }
  const payload = await response.json();
  const row = payload?.chart?.result?.[0];
  if (!row) {
    throw new Error(`Yahoo chart returned empty result for ${symbol}`);
  }
  const meta = row.meta || {};
  const closes = (((row.indicators || {}).quote || [])[0] || {}).close || [];
  const numericCloses = closes.filter((value) => Number.isFinite(value));
  const price = Number.isFinite(meta.regularMarketPrice) ? meta.regularMarketPrice : numericCloses[numericCloses.length - 1];
  const prev = numericCloses.length > 1 ? numericCloses[numericCloses.length - 2] : null;
  const change = Number.isFinite(price) && Number.isFinite(prev) ? price - prev : null;
  const changePercent = Number.isFinite(change) && Number.isFinite(prev) && prev !== 0 ? (change / prev) * 100 : null;
  return {
    price: Number.isFinite(price) ? price : null,
    change,
    change_percent: changePercent,
    market_time: meta.regularMarketTime ? new Date(meta.regularMarketTime * 1000).toISOString() : null
  };
}

async function fetchYahooQuoteSnapshot() {
  const map = [
    { key: 'oil_brent', symbol: 'BZ=F', label: 'النفط (برنت)', unit: 'USD' },
    { key: 'gold_oz', symbol: 'GC=F', label: 'الذهب (أونصة)', unit: 'USD' },
    { key: 'sp500', symbol: '^GSPC', label: 'S&P 500', unit: 'index' },
    { key: 'dxy', symbol: 'DX-Y.NYB', label: 'مؤشر الدولار DXY', unit: 'index' },
    { key: 'us10y', symbol: '^TNX', label: 'عائد السندات الأمريكية 10Y', unit: '%' }
  ];

  const indicators = [];
  for (const item of map) {
    try {
      const quote = await fetchYahooChartQuote(item.symbol);
      indicators.push({
        key: item.key,
        label: item.label,
        symbol: item.symbol,
        unit: item.unit,
        price: quote.price,
        change: quote.change,
        change_percent: quote.change_percent,
        market_time: quote.market_time,
        source: 'Yahoo Finance (Chart API)'
      });
    } catch {
      indicators.push({
        key: item.key,
        label: item.label,
        symbol: item.symbol,
        unit: item.unit,
        price: null,
        change: null,
        change_percent: null,
        market_time: null,
        source: 'Yahoo Finance (Chart API)'
      });
    }
  }

  return {
    updated_at: new Date().toISOString(),
    indicators
  };
}

function isAllowedPath(filePathname) {
  return ALLOWED_STATIC.has(filePathname);
}

async function serveStatic(reqPath, res) {
  const filePathname = reqPath === '/' ? '/index.html' : reqPath;
  if (!isAllowedPath(filePathname)) {
    sendText(res, 404, 'Not Found');
    return;
  }

  const fullPath = path.join(ROOT_DIR, filePathname);
  try {
    const data = await fs.promises.readFile(fullPath);
    const ext = path.extname(fullPath).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME_TYPES[ext] || 'application/octet-stream',
      'Cache-Control': ext === '.html' ? 'no-store' : 'public, max-age=3600'
    });
    res.end(data);
  } catch {
    sendText(res, 404, 'Not Found');
  }
}

function unauthorized(res) {
  sendJson(res, 401, { ok: false, error: 'Unauthorized' });
}

function applyCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Admin-Token');
  res.setHeader('Access-Control-Max-Age', '86400');
}

const server = http.createServer(async (req, res) => {
  ensureDataStore();
  applyCors(res);

  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  const pathname = (url.pathname || '/').replace(/\/+$/g, '') || '/';

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.method === 'GET' && pathname === '/api/news') {
    const payload = await getMergedNews(false);
    sendJson(res, 200, {
      ok: true,
      ...payload
    });
    return;
  }

  if (req.method === 'GET' && pathname === '/api/health') {
    sendJson(res, 200, {
      ok: true,
      uptime_sec: Math.round(process.uptime()),
      cache_age_sec: cache.updatedAt ? Math.round((Date.now() - cache.updatedAt) / 1000) : null,
      has_gemini_key: Boolean(GEMINI_API_KEY),
      admin_password_configured: Boolean(ADMIN_PASSWORD),
      auto_status: getAutoStatusPayload()
    });
    return;
  }

  if (req.method === 'GET' && pathname === '/api/market-snapshot') {
    try {
      const payload = await fetchYahooQuoteSnapshot();
      sendJson(res, 200, { ok: true, ...payload });
    } catch (error) {
      sendJson(res, 200, { ok: false, error: error.message, indicators: [], updated_at: null });
    }
    return;
  }

  if (req.method === 'POST' && pathname === '/api/admin/login') {
    try {
      const body = await parseJsonBody(req);
      const inputPassword = String(body.password || '');
      if (!safeCompare(inputPassword, ADMIN_PASSWORD)) {
        sendJson(res, 401, { ok: false, error: 'Invalid password' });
        return;
      }

      const token = createSessionToken();
      sendJson(res, 200, {
        ok: true,
        token,
        expires_in_sec: Math.round(ADMIN_SESSION_TTL_MS / 1000)
      });
    } catch (error) {
      sendJson(res, 400, { ok: false, error: error.message });
    }
    return;
  }

  if (req.method === 'POST' && pathname === '/api/admin/logout') {
    const token = getAuthToken(req);
    if (token) sessions.delete(token);
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method === 'GET' && pathname === '/api/admin/news') {
    if (!isAuthorized(req)) {
      unauthorized(res);
      return;
    }
    const items = readManualNews().sort(
      (a, b) => new Date(b.created_at || b.published_at || 0) - new Date(a.created_at || a.published_at || 0)
    );
    sendJson(res, 200, { ok: true, items });
    return;
  }

  if (req.method === 'GET' && pathname === '/api/admin/auto-status') {
    if (!isAuthorized(req)) {
      unauthorized(res);
      return;
    }
    sendJson(res, 200, { ok: true, ...getAutoStatusPayload() });
    return;
  }

  if (req.method === 'POST' && pathname === '/api/admin/refresh-ai') {
    if (!isAuthorized(req)) {
      unauthorized(res);
      return;
    }
    const state = await refreshAutoNews(true);
    sendJson(res, 200, {
      ok: true,
      updated_at: state.updatedAt ? new Date(state.updatedAt).toISOString() : null,
      source: state.source,
      error: state.error,
      next_run_at: state.nextRunAt
    });
    return;
  }

  if (req.method === 'POST' && pathname === '/api/admin/refresh-sources') {
    if (!isAuthorized(req)) {
      unauthorized(res);
      return;
    }
    try {
      const body = await parseJsonBody(req);
      const category = String(body.category || 'all').trim();
      const payload = await refreshBySourceCategory(category);
      sendJson(res, 200, { ok: true, ...payload });
    } catch (error) {
      sendJson(res, 400, { ok: false, error: error.message });
    }
    return;
  }

  if (req.method === 'POST' && pathname === '/api/admin/news/extract') {
    if (!isAuthorized(req)) {
      unauthorized(res);
      return;
    }

    try {
      const body = await parseJsonBody(req);
      const sourceUrl = normalizeHttpUrl(body.source_url || '');
      const category = CATEGORIES.includes(body.category) ? body.category : 'local';
      if (!sourceUrl) {
        throw new Error('Valid source_url is required');
      }
      let extracted = null;
      let fallbackUsed = false;
      try {
        extracted = await extractArticleFromUrl(sourceUrl, category);
      } catch {
        extracted = fallbackArticleFromUrl(sourceUrl, category);
        fallbackUsed = true;
      }
      sendJson(res, 200, {
        ok: true,
        fallback_used: fallbackUsed,
        extracted: {
          title: extracted.title,
          summary: extracted.summary,
          source: extracted.source,
          source_url: sourceUrl,
          image_url: extracted.image_url,
          published_at: extracted.published_at,
          scope: extracted.scope
        }
      });
    } catch (error) {
      sendJson(res, 400, { ok: false, error: error.message });
    }
    return;
  }

  if (req.method === 'POST' && pathname === '/api/admin/news') {
    if (!isAuthorized(req)) {
      unauthorized(res);
      return;
    }

    try {
      const body = await parseJsonBody(req);
      const normalized = await normalizeManualItem(body);

      const existing = readManualNews();
      existing.unshift(normalized);
      writeManualNews(existing.slice(0, 1000));

      sendJson(res, 201, { ok: true, item: normalized });
    } catch (error) {
      sendJson(res, 400, { ok: false, error: error.message });
    }
    return;
  }

  if (req.method === 'POST' && pathname === '/api/admin/news/delete') {
    if (!isAuthorized(req)) {
      unauthorized(res);
      return;
    }

    try {
      const body = await parseJsonBody(req);
      const id = String(body.id || '').trim();
      if (!id) {
        throw new Error('id is required');
      }
      const existing = readManualNews();
      const before = existing.length;
      const after = existing.filter((item) => String(item.id || '') !== id);
      if (after.length === before) {
        throw new Error('item not found');
      }
      writeManualNews(after);
      sendJson(res, 200, { ok: true, removed_id: id, remaining: after.length });
    } catch (error) {
      sendJson(res, 400, { ok: false, error: error.message });
    }
    return;
  }

  if (req.method === 'GET') {
    await serveStatic(pathname || '/', res);
    return;
  }

  sendText(res, 405, 'Method Not Allowed');
});

function startServerWithPortFallback() {
  const portsToTry = [PORT, ...PORT_FALLBACKS].filter(
    (value, idx, arr) => Number.isInteger(value) && value > 0 && arr.indexOf(value) === idx
  );

  const startAt = (index) => {
    if (index >= portsToTry.length) {
      console.error(`Unable to bind server. Tried ports: ${portsToTry.join(', ')}`);
      process.exit(1);
      return;
    }

    const targetPort = portsToTry[index];
    server.once('error', (error) => {
      if (error && error.code === 'EADDRINUSE') {
        console.warn(`Port ${targetPort} is busy. Trying next port...`);
        startAt(index + 1);
        return;
      }
      console.error('Server failed to start:', error.message || error);
      process.exit(1);
    });

    server.listen(targetPort, HOST, () => {
      ensureDataStore();
      refreshAutoNews(true).catch(() => {});
      scheduleNextAutoRefresh();
      console.log(`Server running on http://${HOST}:${targetPort}`);
      console.log(`Gemini model: ${GEMINI_MODEL}`);
      console.log(`Auto refresh schedule: ${parseAutoRefreshTimes().map((slot) => `${String(slot.hour).padStart(2, '0')}:${String(slot.minute).padStart(2, '0')}`).join(', ')}`);
      console.log('Admin mode is enabled. Set ADMIN_PASSWORD in .env.');
    });
  };

  startAt(0);
}

startServerWithPortFallback();
