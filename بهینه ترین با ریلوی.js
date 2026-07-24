import { connect } from "cloudflare:sockets";  
  
const GLOBAL_TRAFFIC_CACHE = new Map();  
  
const GLOBAL_DECODER = new TextDecoder();  
  
const GLOBAL_ENCODER = new TextEncoder();  
  
const ACTIVE_CONNECTIONS_COUNT = new Map();  
  
const GLOBAL_LAST_ACTIVE_WRITE = new Map();  
  
const GLOBAL_LAST_DB_WRITE = new Map();  
  
const GLOBAL_WRITE_LOCK = new Map();  
  
const DNS_CACHE = new Map();  
  
const USER_REQ_CACHE = new Map();  
  
const LOGIN_ATTEMPTS = new Map();  
  
const USERS_LIST_CACHE = { data: null, lastFetch: 0 };  
  
const LUT_HEX = Array.from({ length: 256 }, (_, i) =&amp;gt; i.toString(16).padStart(2, '0'));  
  
let GLOBAL_REQ_COUNT = 0;  
  
let GLOBAL_LAST_REQ_WRITE = 0;  
  
const RAILWAY_BACKENDS = [  
  
	"[[makevaslim44-production.up.railway.app](http://makevaslim44-production.up.railway.app)]([http://makevaslim44-production.up.railway.app](http://makevaslim44-production.up.railway.app))",  
  
	"[[seyed84-production.up.railway.app](http://seyed84-production.up.railway.app)]([http://seyed84-production.up.railway.app](http://seyed84-production.up.railway.app))",  
  
	"[[manvaslam-production-07a1.up.railway.app](http://manvaslam-production-07a1.up.railway.app)]([http://manvaslam-production-07a1.up.railway.app](http://manvaslam-production-07a1.up.railway.app))"  
  
];  
  
const DNS_CACHE_TTL = 5  *60*  1000;  
  
const DOH_RESOLVER = "[[https://1.1.1.1/dns-query](https://1.1.1.1/dns-query)](https://1.1.1.1/dns-query](https://1.1.1.1/dns-query))";  
  
const UPSTREAM_BUNDLE_TARGET_BYTES = 32  *1024;*  
  
*const UPSTREAM_QUEUE_MAX_BYTES = 16*  1024*  1024;  
  
const UPSTREAM_QUEUE_MAX_ITEMS = 4096;  
  
const DOWNSTREAM_GRAIN_BYTES = 32  *1024;*  
  
*const DOWNSTREAM_GRAIN_TAIL_THRESHOLD = 512;*  
  
*const DOWNSTREAM_GRAIN_SILENT_MS = 1;*  
  
*const TCP_CONCURRENCY = 4;*  
  
*const PRELOAD_RACE_DIAL = true;*  
  
*const MY_SECRET_DOMAIN = "[[makevaslim.makvasl.workers.dev](http://makevaslim.makvasl.workers.dev)]([http://makevaslim.makvasl.workers.dev](http://makevaslim.makvasl.workers.dev))";*  
  
*async function fetchWithFallback(path, options = {}) {*  
  
	*const githubUrl =* `https://raw.githubusercontent.com/mkvaslim44/Ma_ke_vaslim-Panel/main/${path}`*;*  
  
	*const staticUrl =* `https://raw.githubusercontent.com/mkvaslim44/ZEUS-PANEL/main/${path}`*;*  
  
	*try {*  
  
		*const res = await fetch(githubUrl, options);*  
  
		*if (res.ok) return res;*  
  
	*} catch (e) {}*  
  
	*return await fetch(staticUrl, options);*  
  
*}*  
  
*let localLastAutoResetCheck = 0;*  
  
*async function checkAutoResets(env, ctx) {*  
  
	*const now = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());*  
  
	*if (now - localLastAutoResetCheck &amp;lt; 3600000) return;*  
  
	*try {*  
  
		*const cache = caches.default;*  
  
		*const cacheReq = new Request("[[https://internal.makevaslim/auto_reset](https://internal.makevaslim/auto_reset)](https://internal.makevaslim/auto_reset](https://internal.makevaslim/auto_reset))");*  
  
		*if (await cache.match(cacheReq)) return;*  
  
		*const row = await env.DB.prepare("SELECT value FROM settings WHERE key = 'last_auto_reset_check'").first();*  
  
		*const dbLastCheck = row ? parseInt(row.value) || 0 : 0;*  
  
		*if (now - dbLastCheck &amp;lt; 3600000) {*  
  
			*localLastAutoResetCheck = dbLastCheck;*  
  
			*const ttl = Math.floor((3600000 - (now - dbLastCheck)) / 1000);*  
  
			*if (ttl &amp;gt; 0 &amp;amp;&amp;amp; ctx) ctx.waitUntil(cache.put(cacheReq, new Response("1", { headers: { "Cache-Control":* `max-age=${ttl}` *} })));*  
  
			*return;*  
  
		*}*  
  
		*await env.DB.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_auto_reset_check', ?)").bind(String(now)).run();*  
  
		*localLastAutoResetCheck = now;*  
  
		*if (ctx) ctx.waitUntil(cache.put(cacheReq, new Response("1", { headers: { "Cache-Control": "max-age=3600" } })));*  
  
		*const todayUtc = Math.floor(now / 86400000)*  86400000;  
  
		await env.DB.prepar`UPDATE users SET used_gb = 0, is_active = 1, last_reset_vol_time = ? WHERE auto_reset_vol_days > 0 AND ? >= (last_reset_vol_time + (auto_reset_vol_days  86400000))`*).bind(todayUtc, todayUtc).run();*  
  
		*await env.DB.prepar*`UPDATE users SET used_req = 0, is_active = 1, last_reset_req_time = ? WHERE auto_reset_req_days > 0 AND ? >= (last_reset_req_time + (auto_reset_req_days  86400000))`).bind(todayUtc, todayUtc).run();  
  
	} catch (e) {}  
  
}  
  
let localLastIpRotateCheck = 0;  
  
async function checkAutoRotates(env, ctx) {  
  
	const now = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());  
  
	if (now - localLastIpRotateCheck &amp;lt; 60000) return;  
  
	try {  
  
		const cache = caches.default;  
  
		const cacheReq = new Request("[[https://internal.makevaslim/auto_rotate](https://internal.makevaslim/auto_rotate)](https://internal.makevaslim/auto_rotate](https://internal.makevaslim/auto_rotate))");  
  
		if (await cache.match(cacheReq)) return;  
  
		const row = await env.DB.prepare("SELECT value FROM settings WHERE key = 'last_ip_rotate_check'").first();  
  
		const dbLastCheck = row ? parseInt(row.value) || 0 : 0;  
  
		if (now - dbLastCheck &amp;lt; 60000) {  
  
			localLastIpRotateCheck = dbLastCheck;  
  
			const ttl = Math.floor((60000 - (now - dbLastCheck)) / 1000);  
  
			if (ttl &amp;gt; 0 &amp;amp;&amp;amp; ctx) ctx.waitUntil(cache.put(cacheReq, new Response("1", { headers: { "Cache-Control": `max-age=${ttl}` } })));  
  
			return;  
  
		}  
  
		await env.DB.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_ip_rotate_check', ?)").bind(String(now)).run();  
  
		localLastIpRotateCheck = now;  
  
		if (ctx) ctx.waitUntil(cache.put(cacheReq, new Response("1", { headers: { "Cache-Control": "max-age=60" } })));  
  
		const { results: usersToRotate } = await env.DB.prepare("SELECT  *FROM users WHERE auto_rotate_ip = 1 AND ? &amp;gt;= (last_rotate_time + (rotate_time*  60000))").bind(now).all();  
  
		if (!usersToRotate || usersToRotate.length === 0) return;  
  
		const res = await fetchWithFallback("ips.txt");  
  
		if (!res.ok) return;  
  
		const text = await res.text();  
  
		const blocks = text.split("----------");  
  
		let cachedIpsData = {};  
  
		blocks.forEach((block) =&amp;gt; {  
  
			const lines = block  
  
				.trim()  
  
				.split("\n")  
  
				.map((l) =&amp;gt; l.trim())  
  
				.filter((l) =&amp;gt; l.length &amp;gt; 0);  
  
			if (lines.length === 0) return;  
  
			let opName = "Unknown";  
  
			const ips = [];  
  
			lines.forEach((line) =&amp;gt; {  
  
				if (line.includes("#")) opName = line.split("#")[1].trim();  
  
				else if (!line.startsWith("[source")) ips.push(line);  
  
			});  
  
			if (ips.length &amp;gt; 0) cachedIpsData[opName] = ips;  
  
		});  
  
		const stmts = [];  
  
		for (const u of usersToRotate) {  
  
			let availableIps = [];  
  
			if (u.ip_operator === "all") {  
  
				Object.values(cachedIpsData).forEach((ips) =&amp;gt; (availableIps = availableIps.concat(ips)));  
  
			} else {  
  
				availableIps = cachedIpsData[u.ip_operator] || [];  
  
			}  
  
			availableIps = [...new Set(availableIps)];  
  
			let count = u.ip_count || 20;  
  
			let selectedIps = [];  
  
			if (count &amp;gt;= availableIps.length) {  
  
				selectedIps = availableIps;  
  
			} else {  
  
				const shuffled = availableIps.slice();  
  
				for (let i = shuffled.length - 1; i &amp;gt; 0; i--) {  
  
					const j = Math.floor(Math.random()  *(i + 1));*  
  
					*[shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];*  
  
				*}*  
  
				*selectedIps = shuffled.slice(0, count);*  
  
			*}*  
  
			*if (selectedIps.length &amp;gt; 0) {*  
  
				*stmts.push(env.DB.prepare("UPDATE users SET ips = ?, last_rotate_time = ? WHERE id = ?").bind(selectedIps.join("\n"), now, [[u.id](http://u.id)]([http://u.id](http://u.id))));*  
  
			*}*  
  
		*}*  
  
		*if (stmts.length &amp;gt; 0) {*  
  
			*const batchSize = 50;*  
  
			*for (let i = 0; i &amp;lt; stmts.length; i += batchSize) {*  
  
				*await env.DB.batch(stmts.slice(i, i + batchSize));*  
  
			*}*  
  
		*}*  
  
	*} catch (e) {}*  
  
*}*  
  
*let cachedVipCountries = [];*  
  
*let lastVipCountriesFetch = 0;*  
  
*async function replaceBrokenProxy(username, env, oldProxy) {*  
  
	*try {*  
  
		*if (GLOBAL_WRITE_LOCK.get(username + "_proxy_rotate")) return;*  
  
		*GLOBAL_WRITE_LOCK.set(username + "_proxy_rotate", true);*  
  
		*const user = await env.DB.prepare("SELECT id, user_socks5, auto_rotate_user_proxy FROM users WHERE username = ?").bind(username).first();*  
  
		*if (!user || [[user.auto](http://user.auto)]([http://user.auto)_rotate_user_proxy](http://user.auto)_rotate_user_proxy) !== 1 || user.user_socks5 !== oldProxy) {*  
  
			*GLOBAL_WRITE_LOCK.delete(username + "_proxy_rotate");*  
  
			*return;*  
  
		*}*  
  
		*let countryCode = "all";*  
  
		*try {*  
  
			*let remain = oldProxy.replace(/^(socks4|socks5|socks|http|https):\/\//i, "");*  
  
			*if (remain.includes("@")) remain = remain.substring(remain.lastIndexOf("@") + 1);*  
  
			*if (remain.startsWith("[")) remain = remain.substring(1, remain.indexOf("]"));*  
  
			*else if (remain.includes(":")) remain = remain.substring(0, remain.lastIndexOf(":"));*  
  
			*const geoRes = await fetc*`http://ip-api.com/json/${remain}?fields=countryCode`*);*  
  
			*const geoData = await geoRes.json();*  
  
			*if (geoData &amp;amp;&amp;amp; geoData.countryCode) countryCode = geoData.countryCode;*  
  
		*} catch (e) {}*  
  
		*let newProxy = null;*  
  
		*const upperCountry = countryCode.toUpperCase();*  
  
		*const sources = [];*  
  
		*const isOldProxyVIP = oldProxy.includes("@");*  
  
		*if (cachedVipCountries.length === 0 || [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)()) - lastVipCountriesFetch &amp;gt; 3600000) {*  
  
			*try {*  
  
				*const ghRes = await fetchWithFallback("vip-list", {*  
  
					*headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" }*  
  
				*});*  
  
				*if (ghRes.ok) {*  
  
					*const files = await ghRes.json();*  
  
					*cachedVipCountries = files.filter(f =&amp;gt; [[f.name](http://f.name)]([http://f.name).endsWith('.txt')).map(f](http://f.name).endsWith('.txt')).map(f) =&amp;gt; [[f.name](http://f.name)]([http://f.name).replace('.txt](http://f.name).replace('.txt)', '').toUpperCase());*  
  
					*lastVipCountriesFetch = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());*  
  
				*}*  
  
			*} catch (e) {}*  
  
		*}*  
  
		*let fallbackVIPs = cachedVipCountries.length &amp;gt; 0 ? [...cachedVipCountries] : ["DE", "US", "GB", "NL", "FR", "TR"];*  
  
		*for (let i = fallbackVIPs.length - 1; i &amp;gt; 0; i--) {*  
  
			*const j = Math.floor(Math.random()*  (i + 1));  
  
			[fallbackVIPs[i], fallbackVIPs[j]] = [fallbackVIPs[j], fallbackVIPs[i]];  
  
		}  
  
		if (upperCountry !== "ALL" &amp;amp;&amp;amp; upperCountry !== "UN") {  
  
			sources.push({ url: `proxy_vip/${upperCountry}.txt`, type: 'repo' });  
  
		}  
  
		for (const fc of fallbackVIPs) {  
  
			if (fc !== upperCountry) {  
  
				sources.push({ url: `proxy_vip/${fc}.txt`, type: 'repo' });  
  
			}  
  
		}  
  
		if (!isOldProxyVIP) {  
  
			if (upperCountry !== "ALL" &amp;amp;&amp;amp; upperCountry !== "UN") {  
  
				sources.push({ url: `proxy/${upperCountry}.txt`, type: 'repo' });  
  
			}  
  
			sources.push({ url: `proxy/ALL.txt`, type: 'repo' });  
  
		}  
  
		for (const src of sources) {  
  
			try {  
  
				const res = await fetchWithFallback(src.url);  
  
				if (!res.ok) continue;  
  
				const text = await res.text();  
  
				const lines = text.split("\n").map(l =&amp;gt; l.trim()).filter(l =&amp;gt; l.length &amp;gt; 5);  
  
				if (lines.length &amp;gt; 0) {  
  
					for (let i = lines.length - 1; i &amp;gt; 0; i--) {  
  
						const j = Math.floor(Math.random()  *(i + 1));*  
  
						*[lines[i], lines[j]] = [lines[j], lines[i]];*  
  
					*}*  
  
					*const testBatch = lines.slice(0, 3).flatMap(line =&amp;gt; {*  
  
						*if (line.match(/^(socks4|socks5|socks|http|https|tg):\/\//i) || line.includes("t.me/socks")) {*  
  
							*return [line];*  
  
						*}*  
  
						*if (src.type === 'socks5') return* `socks5://${line}`*];*  
  
						*if (src.type === 'http') return* `http://${line}`*];*  
  
						*return* `socks5://${line}`*,* `http://${line}`*];*  
  
					*});*  
  
					*try {*  
  
						*newProxy = await Promise.any([[testBatch.map](http://testBatch.map)]([http://testBatch.map)(p](http://testBatch.map)(p) =&amp;gt; {*  
  
							*return new Promise(async (resolve, reject) =&amp;gt; {*  
  
								*const timeoutId = setTimeout(() =&amp;gt; reject(new Error('timeout')), 3000); *  
  
								*try {*  
  
									*const payload = new TextEncoder().encode("GET / HTTP/1.1\r\nHost: 1.1.1.1\r\nConnection: close\r\n\r\n");*  
  
									*const s = await connectProxy(p, "1.1.1.1", 80, payload);*  
  
									*const reader = s.readable.getReader();*  
  
									*const res = await [[reader.read](http://reader.read)]([http://reader.read)()](http://reader.read)());*  
  
									*s.close();*  
  
									*clearTimeout(timeoutId);*  
  
									*if (res.done || !res.value) reject(new Error("empty"));*  
  
									*else resolve(p);*  
  
								*} catch (e) {*  
  
									*clearTimeout(timeoutId);*  
  
									*reject(e);*  
  
								*}*  
  
							*});*  
  
						*}));*  
  
					*} catch (e) {*  
  
						*continue;*  
  
					*}*  
  
					*if (newProxy) {*  
  
						*break; *  
  
					*}*  
  
				*}*  
  
			*} catch (e) {}*  
  
		*}*  
  
		*if (newProxy) {*  
  
			*await env.DB.prepare("UPDATE users SET user_socks5 = ? WHERE id = ?").bind(newProxy, [[user.id](http://user.id)]([http://user.id)).run()](http://user.id)).run());*  
  
		*}*  
  
	*} catch(e) {*  
  
	*} finally {*  
  
		*GLOBAL_WRITE_LOCK.delete(username + "_proxy_rotate");*  
  
	*}*  
  
*}*  
  
*export default {*  
  
	*async fetch(request, env, ctx) {*  
  
		*if (!env.DB) {*  
  
			*return new Response("Database binding 'DB' is missing in Cloudflare Workers settings.", { status: 500 });*  
  
		*}*  
  
		*await DbService.ensureSchema(env.DB);*  
  
		*trackRequest(env, ctx);*  
  
		*if (schemaEnsured) {*  
  
			*ctx.waitUntil(checkAutoResets(env, ctx));*  
  
			*ctx.waitUntil(checkAutoRotates(env, ctx));*  
  
		*}*  
  
		*const url = new URL(request.url);*  
  
		*if (url.pathname === "/api/init-db") {*  
  
			*try {*  
  
				*await DbService.ensureSchema(env.DB);*  
  
				*return new Response("Database initialized successfully.", {*  
  
					*status: 200,*  
  
					*headers: { "Content-Type": "text/plain; charset=utf-8" }*  
  
				*});*  
  
			*} catch (err) {*  
  
				*return new Respons*`Database init failed: ${err.message}`*, { status: 500 });*  
  
			*}*  
  
		*}*  
  
		*if (Router.isWebSocketUpgrade(request) &amp;amp;&amp;amp; url.pathname.startsWith("/railway-io/")) {*  
  
			*const extractedUuid = url.pathname.split("/")[2];*  
  
			*if (extractedUuid) {*  
  
				*return await handleRailwayWS(request, env, ctx, extractedUuid);*  
  
			*}*  
  
		*}*  
  
		*if (Router.isWebSocketUpgrade(request) &amp;amp;&amp;amp; url.pathname.startsWith("/p/")) {*  
  
			*const pathParts = url.pathname.split("/");*  
  
			*if (pathParts.length &amp;gt;= 4 &amp;amp;&amp;amp; pathParts[3] === "Ma_Ke_Vaslim") {*  
  
				*try {*  
  
					*const proxyB64 = pathParts[2];*  
  
					*const decodedProxy = decodeURIComponent(atob(proxyB64));*  
  
					*return await Router.handleWebSocket(request, env, ctx, decodedProxy);*  
  
				*} catch (e) {}*  
  
			*}*  
  
			*return await Router.handleWebSocket(request, env, ctx);*  
  
		*}*  
  
		*if (Router.isWebSocketUpgrade(request) &amp;amp;&amp;amp; url.pathname === "/Ma_Ke_Vaslim") {*  
  
			*return await Router.handleWebSocket(request, env, ctx);*  
  
		*}*  
  
		*if (Router.isWebSocketUpgrade(request)) {*  
  
			*return await Router.handleWebSocket(request, env, ctx);*  
  
		*}*  
  
		*if (Router.isSubscriptionPath(url.pathname)) {*  
  
			*return await Router.handleSubscription(url, env);*  
  
		*}*  
  
		*if (url.pathname.startsWith("/api/") || url.pathname === "/locations") {*  
  
			*return await Router.handleApi(request, url, env, ctx);*  
  
		*}*  
  
		*if (url.pathname === "/panel" || url.pathname === "/login") {*  
  
			*return await Router.handlePanel(request, env);*  
  
		*}*  
  
		*if (url.pathname.startsWith("/status/")) {*  
  
			*return await Router.handleUserStatus(url, env, request);*  
  
		*}*  
  
		*return new Response(HTML_TEMPLATES.nginx, {*  
  
			*headers: { "Content-Type": "text/html; charset=utf-8" },*  
  
		*});*  
  
	*},*  
  
	*async scheduled(_event, env, ctx) {*  
  
		*ctx.waitUntil(*  
  
			*(async () =&amp;gt; {*  
  
				*try {*  
  
					*await env.DB.prepare("UPDATE users SET used_req = 0").run();*  
  
					*if (typeof USER_REQ_CACHE !== 'undefined' &amp;amp;&amp;amp; USER_REQ_CACHE.clear) {*  
  
						*USER_REQ_CACHE.clear();*  
  
					*}*  
  
					*if (typeof USERS_LIST_CACHE !== 'undefined') {*  
  
						*USERS_LIST_[[CACHE.data](http://CACHE.data)]([http://CACHE.data](http://CACHE.data)) = null;*  
  
						*USERS_LIST_CACHE.lastFetch = 0;*  
  
					*}*  
  
					*console.log("✅ ریکوئست‌های روزانه تمامی کاربران با موفقیت ریست شد.");*  
  
				*} catch (error) {*  
  
					*console.error("❌ خطا در ریست روزانه ریکوئست‌ها:", error);*  
  
				*}*  
  
			*})()*  
  
		*);*  
  
	*}*  
  
*};*  
  
*const Router = {*  
  
	*isWebSocketUpgrade(request) {*  
  
		*const upgradeHeader = (request.headers.get("Upgrade") || "").toLowerCase();*  
  
		*return upgradeHeader === "websocket";*  
  
	*},*  
  
	*isSubscriptionPath(pathname) {*  
  
		*return pathname.startsWith("/sub/") || pathname.startsWith("/feed/");*  
  
	*},*  
  
	*async handleWebSocket(_request, env, ctx, perIpProxy = null) {*  
  
		*try {*  
  
			*let proxyIP = "[[proxyip.cmliussss.net](http://proxyip.cmliussss.net)]([http://proxyip.cmliussss.net](http://proxyip.cmliussss.net))";*  
  
			*try {*  
  
				*const proxyRow = await env.DB.prepare("SELECT value FROM settings WHERE key = 'proxy_ip'").first();*  
  
				*if (proxyRow &amp;amp;&amp;amp; proxyRow.value) {*  
  
					*proxyIP = proxyRow.value;*  
  
				*}*  
  
			*} catch (e) {}*  
  
			*const mockStoredData = { proxy_ip: proxyIP, per_ip_proxy: perIpProxy };*  
  
			*return handleVLESS(env, mockStoredData, ctx, request);*  
  
		*} catch (e) {*  
  
			*return new Response("Internal Server Error", { status: 500 });*  
  
		*}*  
  
	*},*  
  
	*async handleSubscription(url, env) {*  
  
		*const isSubPath = url.pathname.startsWith("/sub/");*  
  
		*const offset = isSubPath ? 5 : 6;*  
  
		*let subUser = decodeURIComponent(url.pathname.slice(offset));*  
  
		*const host = url.hostname;*  
  
		*try {*  
  
			*const user = await env.DB.prepare("SELECT  FROM users WHERE username = ? OR uuid = ?").bind(subUser, subUser).first();*  
  
			*if (!user || user.connection*type !== "vl" + "e" + "ss") {  
  
				return new Response("Not Found", { status: 404 });  
  
			}  
  
			try {  
  
				await env.DB.prepare("UPDATE users SET used_req = used_req + 1 WHERE username = ?").bind(user.username).run();  
  
			} catch (e) {}  
  
			return await SubscriptionService.generateText(user, host);  
  
		} catch (err) {  
  
			return new Response("Error building config: " + err.message, { status: 500 });  
  
		}  
  
	},  
  
	async handlePanel(request, env) {  
  
		const hasPassword = await DbService.getPanelPassword(env.DB);  
  
		if (!hasPassword) {  
  
			return new Response(HTML_TEMPLATES.setup, {  
  
				headers: { "Content-Type": "text/html; charset=utf-8" },  
  
			});  
  
		}  
  
		const authorized = await DbService.verifyApiAuth(request, env);  
  
		if (!authorized) {  
  
			return new Response(HTML_TEMPLATES.login, {  
  
				headers: { "Content-Type": "text/html; charset=utf-8" },  
  
			});  
  
		}  
  
		return new Response(HTML_TEMPLATES.panel, {  
  
			headers: {  
  
				"Content-Type": "text/html; charset=utf-8",  
  
				"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",  
  
				Pragma: "no-cache",  
  
				Expires: "0",  
  
			},  
  
		});  
  
	},  
  
	async handleUserStatus(url, env, request) {  
  
		const username = decodeURIComponent(url.pathname.slice(8));  
  
		if (!username) {  
  
			return new Response("Username is required", { status: 400 });  
  
		}  
  
		try {  
  
			const user = await env.DB.prepare("SELECT  *FROM users WHERE username = ? OR uuid = ?").bind(username, username).first();*  
  
			*if (!user) {*  
  
				*return new Response("User not found", { status: 404 });*  
  
			*}*  
  
			*const acceptHeader = request.headers.get("Accept") || "";*  
  
			*const isBrowser = acceptHeader.includes("text/html");*  
  
			*if (isBrowser) {*  
  
				*const userJson = JSON.stringify({*  
  
					*username: user.username,*  
  
					*uuid: user.uuid,*  
  
					*limit_gb: user.limit_gb,*  
  
					*expiry_days: user.expiry_days,*  
  
					*used_gb: user.used_gb,*  
  
					*limit_req: user.limit_req,*  
  
					*used_req: user.used_req,*  
  
					*is_active: [[user.is](http://user.is)]([http://user.is)_active](http://user.is)_active),*  
  
					*online_count: getActiveIpCount(user.active_ips),*  
  
					*ip_limit: user.ip_limit,*  
  
					*created_at: user.created_at,*  
  
					*tls: user.tls,*  
  
					*port: user.port,*  
  
					*ips: user.ips,*  
  
					*fingerprint: user.fingerprint || "chrome",*  
  
					*user_proxy_iata: user.user_proxy_iata,*  
  
					*user_socks5: user.user_socks5,*  
  
					*user_proxy_ip: user.user_proxy_ip,*  
  
				*});*  
  
				*const html = HTML_TEMPLATES.status.replace("/* {{USER_DATA_PLACEHOLDER}} */",* `window.statusUser = ${userJson};`*);*  
  
				*return new Response(html, {*  
  
					*headers: { "Content-Type": "text/html; charset=utf-8" },*  
  
				*});*  
  
			*} else {*  
  
				*return await SubscriptionService.generateText(user, url.hostname);*  
  
			*}*  
  
		*} catch (err) {*  
  
			*return new Response("Error: " + err.message, { status: 500 });*  
  
		*}*  
  
	*},*  
  
	*async handleApi(request, url, env, ctx) {*  
  
		*const hasPassword = await DbService.getPanelPassword(env.DB);*  
  
		*if (url.pathname === "/api/setup-password" &amp;amp;&amp;amp; request.method === "POST") {*  
  
			*if (hasPassword) {*  
  
				*return new Response(JSON.stringify({ error: "رمز عبور از قبل تعریف شده است" }), {*  
  
					*status: 400,*  
  
					*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
				*});*  
  
			*}*  
  
			*const { password } = await request.json();*  
  
			*if (!password || password.length &amp;lt; 4) {*  
  
				*return new Response(JSON.stringify({ error: "رمز عبور باید حداقل ۴ کاراکتر باشد" }), {*  
  
					*status: 400,*  
  
					*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
				*});*  
  
			*}*  
  
			*const hashed = await DbService.sha256(password);*  
  
			*await DbService.setPanelPassword(env.DB, hashed);*  
  
			*return new Response(JSON.stringify({ success: true }), {*  
  
				*headers: {*  
  
					*"Content-Type": "application/json; charset=utf-8",*  
  
					*"Set-Cookie": "panel_session=" + hashed + "; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=31536000",*  
  
				*},*  
  
			*});*  
  
		*}*  
  
		*if (url.pathname === "/api/login" &amp;amp;&amp;amp; request.method === "POST") {*  
  
			*const clientIP = request.headers.get("CF-Connecting-IP") || "unknown";*  
  
			*const now = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());*  
  
			*const attemptRecord = LOGIN_ATTEMPTS.get(clientIP) || { count: 0, lastAttempt: 0 };*  
  
			*if (attemptRecord.count &amp;gt;= 5 &amp;amp;&amp;amp; (now - attemptRecord.lastAttempt) &amp;lt; 900000) {*  
  
				*const remaining = Math.ceil((900000 - (now - attemptRecord.lastAttempt)) / 60000);*  
  
				*return new Response(JSON.stringify({ error:* `دسترسی شما مسدود شد. لطفاً ${remaining} دقیقه دیگر تلاش کنید.` *}), {*  
  
					*status: 429,*  
  
					*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
				*});*  
  
			*}*  
  
			*const { password } = await request.json();*  
  
			*const hashedInput = await DbService.sha256(password);*  
  
			*const storedHash = await DbService.getPanelPassword(env.DB);*  
  
			*let isValid = false;*  
  
			*if (storedHash === hashedInput) {*  
  
				*isValid = true;*  
  
			*} else {*  
  
				*const oldHashedInput = await DbService.oldSha256(password);*  
  
				*if (storedHash === oldHashedInput) {*  
  
					*isValid = true;*  
  
					*await DbService.setPanelPassword(env.DB, hashedInput);*  
  
				*}*  
  
			*}*  
  
			*if (isValid) {*  
  
				*LOGIN_ATTEMPTS.delete(clientIP); *  
  
				*return new Response(JSON.stringify({ success: true }), {*  
  
					*headers: {*  
  
						*"Content-Type": "application/json; charset=utf-8",*  
  
						*"Set-Cookie": "panel_session=" + hashedInput + "; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=31536000",*  
  
					*},*  
  
				*});*  
  
			*} else {*  
  
				*attemptRecord.count = (now - attemptRecord.lastAttempt &amp;gt; 900000) ? 1 : attemptRecord.count + 1;*  
  
				*attemptRecord.lastAttempt = now;*  
  
				*LOGIN_ATTEMPTS.set(clientIP, attemptRecord);*  
  
				**  
  
				*return new Response(JSON.stringify({ error:* `رمز عبور اشتباه است (تلاش‌های باقی‌مانده: ${5 - attemptRecord.count})` *}), {*  
  
					*status: 401,*  
  
					*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
				*});*  
  
			*}*  
  
		*}*  
  
		*if (url.pathname === "/api/logout" &amp;amp;&amp;amp; request.method === "POST") {*  
  
			*return new Response(JSON.stringify({ success: true }), {*  
  
				*headers: {*  
  
					*"Content-Type": "application/json; charset=utf-8",*  
  
					*"Set-Cookie": "panel_session=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; Secure; SameSite=Lax",*  
  
				*},*  
  
			*});*  
  
		*}*  
  
		*if (url.pathname === "/api/recover" &amp;amp;&amp;amp; request.method === "POST") {*  
  
			*const { api_token } = await request.json();*  
  
			*if (!api_token) {*  
  
				*return new Response(JSON.stringify({ error: "Token is required" }), {*  
  
					*status: 400,*  
  
					*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
				*});*  
  
			*}*  
  
			*try {*  
  
				*const cfRes = await fetch("[[https://api.cloudflare.com/client/v4/user/tokens/verify](https://api.cloudflare.com/client/v4/user/tokens/verify)](https://api.cloudflare.com/client/v4/user/tokens/verify](https://api.cloudflare.com/client/v4/user/tokens/verify))", {*  
  
					*headers: { Authorization: "Bearer " + api_token },*  
  
				*});*  
  
				*const cfData = await cfRes.json();*  
  
				*if (!cfRes.ok || !cfData.success) {*  
  
					*return new Response(JSON.stringify({ error: "Invalid or expired Cloudflare token" }), {*  
  
						*status: 401,*  
  
						*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
					*});*  
  
				*}*  
  
				*const host = url.hostname;*  
  
				*let isAuthorized = false;*  
  
				*if (host.endsWith(".[[workers.dev](http://workers.dev)]([http://workers.dev](http://workers.dev))")) {*  
  
					*const parts = host.split(".");*  
  
					*const targetSubdomain = parts[parts.length - 3];*  
  
					*const accountsRes = await fetch("[[https://api.cloudflare.com/client/v4/accounts](https://api.cloudflare.com/client/v4/accounts)](https://api.cloudflare.com/client/v4/accounts](https://api.cloudflare.com/client/v4/accounts))", {*  
  
						*headers: { Authorization: "Bearer " + api_token },*  
  
					*});*  
  
					*const accountsData = await accountsRes.json();*  
  
					*if (accountsData.success &amp;amp;&amp;amp; accountsData.result) {*  
  
						*for (const acc of accountsData.result) {*  
  
							*const subRes = await fetc*`https://api.cloudflare.com/client/v4/accounts/${acc.id}/workers/subdomain`*, {*  
  
								*headers: { Authorization: "Bearer " + api_token },*  
  
							*});*  
  
							*const subData = await subRes.json();*  
  
							*if (subData.success &amp;amp;&amp;amp; subData.result &amp;amp;&amp;amp; subData.result.subdomain === targetSubdomain) {*  
  
								*isAuthorized = true;*  
  
								*break;*  
  
							*}*  
  
						*}*  
  
					*}*  
  
				*} else {*  
  
					*const zonesRes = await fetch("[[https://api.cloudflare.com/client/v4/zones](https://api.cloudflare.com/client/v4/zones)](https://api.cloudflare.com/client/v4/zones](https://api.cloudflare.com/client/v4/zones))", {*  
  
						*headers: { Authorization: "Bearer " + api_token },*  
  
					*});*  
  
					*const zonesData = await zonesRes.json();*  
  
					*if (zonesData.success &amp;amp;&amp;amp; zonesData.result) {*  
  
						*for (const zone of zonesData.result) {*  
  
							*if (host === [[zone.name](http://zone.name)]([http://zone.name](http://zone.name)) || host.endsWith("." + [[zone.name](http://zone.name)]([http://zone.name](http://zone.name)))) {*  
  
								*isAuthorized = true;*  
  
								*break;*  
  
							*}*  
  
						*}*  
  
					*}*  
  
				*}*  
  
				*if (!isAuthorized) {*  
  
					*return new Response(JSON.stringify({ error: "این توکن متعلق به صاحب پـنـل نیست (ای کــثـــکـــش)" }), {*  
  
						*status: 403,*  
  
						*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
					*});*  
  
				*}*  
  
				*await env.DB.prepare("DELETE FROM settings WHERE key = 'panel_password'").run();*  
  
				*cachedPanelPassword = null;*  
  
				*return new Response(JSON.stringify({ success: true }), {*  
  
					*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
				*});*  
  
			*} catch (err) {*  
  
				*return new Response(JSON.stringify({ error: "Cloudflare API connection error" }), {*  
  
					*status: 500,*  
  
					*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
				*});*  
  
			*}*  
  
		*}*  
  
		*const authorized = await DbService.verifyApiAuth(request, env);*  
  
		*if (!authorized &amp;amp;&amp;amp; url.pathname !== "/api/test-proxy") {*  
  
			*return new Response(JSON.stringify({ error: "Unauthorized" }), {*  
  
				*status: 401,*  
  
				*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
			*});*  
  
		*}*  
  
		*if (url.pathname === "/api/restart-core" &amp;amp;&amp;amp; request.method === "POST") {*  
  
			*try {*  
  
				*GLOBAL_TRAFFIC_CACHE.clear();*  
  
				*ACTIVE_CONNECTIONS_COUNT.clear();*  
  
				*GLOBAL_LAST_ACTIVE_WRITE.clear();*  
  
				*GLOBAL_LAST_DB_WRITE.clear();*  
  
				*GLOBAL_WRITE_LOCK.clear();*  
  
				*DNS_CACHE.clear();*  
  
				*USER_REQ_CACHE.clear();*  
  
				*return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });*  
  
			*} catch (err) {*  
  
				*return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { "Content-Type": "application/json" } });*  
  
			*}*  
  
		*}*  
  
		*if (url.pathname === "/api/update-panel" &amp;amp;&amp;amp; request.method === "POST") {*  
  
			*const body = await request.json().catch(() =&amp;gt; ({}));*  
  
			*let currentToken = [[env.CF](http://env.CF)]([http://env.CF)_API_TOKEN](http://env.CF)_API_TOKEN) || [[body.cf](http://body.cf)]([http://body.cf)_token](http://body.cf)_token) || null;*  
  
			*let currentAccountId = [[env.CF](http://env.CF)]([http://env.CF)_ACCOUNT_ID](http://env.CF)_ACCOUNT_ID);*  
  
			*if (!currentToken) {*  
  
				*return new Response(JSON.stringify({ error: "TOKEN_REQUIRED" }), { status: 400, headers: { "Content-Type": "application/json" } });*  
  
			*}*  
  
			*try {*  
  
				*const cfHeaders = {*  
  
					*"Authorization": "Bearer " + currentToken,*  
  
					*"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ZeusPanel/1.0"*  
  
				*};*  
  
				*if (!currentAccountId) {*  
  
					*const accRes = await fetch("[[https://api.cloudflare.com/client/v4/accounts](https://api.cloudflare.com/client/v4/accounts)](https://api.cloudflare.com/client/v4/accounts](https://api.cloudflare.com/client/v4/accounts))", { headers: cfHeaders });*  
  
					*if (!accRes.ok) throw new Error("کلودفلر درخواست اکانت را رد کرد (وضعیت: " + accRes.status + ")");*  
  
					*const accData = await accRes.json().catch(() =&amp;gt; ({}));*  
  
					*if (!accData.success || !accData.result || accData.result.length === 0) throw new Error("توکن نامعتبر است یا اکانتی یافت نشد.");*  
  
					*currentAccountId = accData.result[0].id;*  
  
				*}*  
  
				**  
  
				*const githubRes = await fetch("[[https://raw.githubusercontent.com/mkvaslim44/Ma_ke_vaslim-Panel/main/panel-source?t=](https://raw.githubusercontent.com/mkvaslim44/Ma_ke_vaslim-Panel/main/panel-source?t=)](https://raw.githubusercontent.com/mkvaslim44/Ma_ke_vaslim-Panel/main/panel-source?t=](https://raw.githubusercontent.com/mkvaslim44/Ma_ke_vaslim-Panel/main/panel-source?t=))" + [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)()), {*  
  
					*headers: {*  
  
						*"User-Agent": "Mozilla/5.0",*  
  
						*"Cache-Control": "no-cache"*  
  
					*}*  
  
				*});*  
  
				*if (!githubRes.ok) throw new Error("خطا در دریافت سورس جدید از گیت‌هاب (وضعیت: " + githubRes.status + ")");*  
  
				*const newCode = await githubRes.text();*  
  
				*const scriptName = env.WORKER_NAME || url.hostname.split(".")[0];*  
  
				**  
  
				*const bindingsRes = await fetc*`https://api.cloudflare.com/client/v4/accounts/${currentAccountId}/workers/scripts/${scriptName}/bindings`*, {*  
  
					*headers: cfHeaders*  
  
				*});*  
  
				*if (!bindingsRes.ok) throw new Error("عدم دسترسی به تنظیمات ورکر. کلودفلر خطا داد (وضعیت: " + bindingsRes.status + ")");*  
  
				*const bindingsData = await bindingsRes.json().catch(() =&amp;gt; ({}));*  
  
				*if (!bindingsData.success) throw new Error("توکن فاقد دسترسی ویرایش ورکر است.");*  
  
				**  
  
				*const newBindings = [];*  
  
				*for (const b of bindingsData.result || []) {*  
  
					*if ([[b.name](http://b.name)]([http://b.name](http://b.name)) === "CF_API_TOKEN" || [[b.name](http://b.name)]([http://b.name](http://b.name)) === "CF_ACCOUNT_ID") continue;*  
  
					*if (b.type === "d1") {*  
  
						*newBindings.push({ type: "d1", name: [[b.name](http://b.name)]([http://b.name](http://b.name)), id: b.database_id || [[b.id](http://b.id)]([http://b.id](http://b.id)) });*  
  
					*} else if (b.type === "kv_namespace") {*  
  
						*newBindings.push({ type: "kv_namespace", name: [[b.name](http://b.name)]([http://b.name](http://b.name)), namespace_id: b.namespace_id || [[b.id](http://b.id)]([http://b.id](http://b.id)) });*  
  
					*} else if (b.type === "plain_text") {*  
  
						*newBindings.push({ type: "plain_text", name: [[b.name](http://b.name)]([http://b.name](http://b.name)), text: b.text || "" });*  
  
					*} else if (b.type !== "secret_text") {*  
  
						*newBindings.push(b);*  
  
					*}*  
  
				*}*  
  
				*newBindings.push({ type: "secret_text", name: "CF_API_TOKEN", text: currentToken });*  
  
				*newBindings.push({ type: "secret_text", name: "CF_ACCOUNT_ID", text: currentAccountId });*  
  
				**  
  
				*const metadata = {*  
  
					*main_module: "zeus.js",*  
  
					*compatibility_date: "2026-07-10",*  
  
					*compatibility_flags: ["nodejs_compat"],*  
  
					*bindings: newBindings*  
  
				*};*  
  
				**  
  
				*const formData = new FormData();*  
  
				*formData.append("metadata", new Blob([JSON.stringify(metadata)], { type: "application/json" }), "metadata.json");*  
  
				*formData.append("zeus.js", new Blob([newCode], { type: "application/javascript+module" }), "zeus.js");*  
  
				**  
  
				*const deployRes = await fetc*`https://api.cloudflare.com/client/v4/accounts/${currentAccountId}/workers/scripts/${scriptName}`*, {*  
  
					*method: "PUT",*  
  
					*headers: cfHeaders,*  
  
					*body: formData*  
  
				*});*  
  
				*if (!deployRes.ok) {*  
  
					*const errText = await deployRes.text().catch(() =&amp;gt; "");*  
  
					*throw new Error("خطای کلودفلر هنگام دیپلوی (" + deployRes.status + "): " + errText.substring(0, 150));*  
  
				*}*  
  
				*const deployData = await deployRes.json().catch(() =&amp;gt; ({}));*  
  
				*if (!deployData.success) {*  
  
					*const cfError = deployData.errors &amp;amp;&amp;amp; deployData.errors.length &amp;gt; 0 ? deployData.errors[0].message : "خطا در اعمال آپدیت.";*  
  
					*throw new Error(cfError);*  
  
				*}*  
  
				*return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });*  
  
			*} catch (err) {*  
  
				*return new Response(JSON.stringify({ error: err.message }), { status: 400, headers: { "Content-Type": "application/json" } });*  
  
			*}*  
  
		*}*  
  
		*if (url.pathname === "/api/change-password" &amp;amp;&amp;amp; request.method === "POST") {*  
  
			*const { current_password, new_password } = await request.json();*  
  
			*if (!current_password || !new_password) {*  
  
				*return new Response(JSON.stringify({ error: "رمز عبور فعلی و جدید الزامی هستند" }), {*  
  
					*status: 400,*  
  
					*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
				*});*  
  
			*}*  
  
			*const currentHash = await DbService.sha256(current_password);*  
  
			*const oldCurrentHash = await DbService.oldSha256(current_password);*  
  
			*const storedHash = await DbService.getPanelPassword(env.DB);*  
  
			**  
  
			*if (storedHash &amp;amp;&amp;amp; storedHash !== currentHash &amp;amp;&amp;amp; storedHash !== oldCurrentHash) {*  
  
				*return new Response(JSON.stringify({ error: "رمز عبور فعلی اشتباه است" }), {*  
  
					*status: 401,*  
  
					*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
				*});*  
  
			*}*  
  
			*if (new_password.length &amp;lt; 4) {*  
  
				*return new Response(JSON.stringify({ error: "رمز عبور جدید باید حداقل ۴ کاراکتر باشد" }), {*  
  
					*status: 400,*  
  
					*headers: { "Content-Type": "application/json; charset=utf-8" },*  
  
				*});*  
  
			*}*  
  
			*const newHash = await DbService.sha256(new_password);*  
  
			*await DbService.setPanelPassword(env.DB, newHash);*  
  
			*return new Response(JSON.stringify({ success: true }), {*  
  
				*headers: {*  
  
					*"Content-Type": "application/json; charset=utf-8",*  
  
					*"Set-Cookie": "panel_session=" + newHash + "; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=31536000",*  
  
				*},*  
  
			*});*  
  
		*}*  
  
		*if (url.pathname === "/api/settings/bulk") {*  
  
			*if (request.method === "GET") {*  
  
				*try {*  
  
					*const { results } = await env.DB.prepare("SELECT*  FROM settings").all();  
  
					const settingsObj = {};  
  
					if (results) {  
  
						results.forEach((r) =&amp;gt; {  
  
							settingsObj[r.key] = r.value;  
  
						});  
  
					}  
  
					return new Response(JSON.stringify(settingsObj), { headers: { "Content-Type": "application/json" } });  
  
				} catch (e) {  
  
					return new Response(JSON.stringify({}), { headers: { "Content-Type": "application/json" } });  
  
				}  
  
			}  
  
			if (request.method === "POST") {  
  
				const body = await request.json();  
  
				if (body.settings &amp;amp;&amp;amp; typeof body.settings === "object") {  
  
					for (const [k, v] of Object.entries(body.settings)) {  
  
						await env.DB.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)").bind(k, String(v)).run();  
  
					}  
  
				}  
  
				return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });  
  
			}  
  
		}  
  
		if (url.pathname === "/api/proxy-ip") {  
  
			if (request.method === "POST") {  
  
				const { proxy_ip, iata, socks5 } = await request.json();  
  
				if (proxy_ip) await env.DB.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('proxy_ip', ?)").bind(proxy_ip).run();  
  
				if (iata !== undefined) await env.DB.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('proxy_location_iata', ?)").bind(iata).run();  
  
				if (socks5 !== undefined) await env.DB.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('socks5', ?)").bind(socks5).run();  
  
				return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });  
  
			}  
  
			if (request.method === "GET") {  
  
				const rowIp = await env.DB.prepare("SELECT value FROM settings WHERE key = 'proxy_ip'").first();  
  
				const rowIata = await env.DB.prepare("SELECT value FROM settings WHERE key = 'proxy_location_iata'").first();  
  
				const rowSocks = await env.DB.prepare("SELECT value FROM settings WHERE key = 'socks5'").first();  
  
				return new Response(  
  
					JSON.stringify({  
  
						proxy_ip: rowIp ? rowIp.value : "",  
  
						iata: rowIata ? rowIata.value : "",  
  
						socks5: rowSocks ? rowSocks.value : "",  
  
					}),  
  
					{ headers: { "Content-Type": "application/json" } },  
  
				);  
  
			}  
  
		}  
  
		if (url.pathname === "/api/test-proxy" &amp;amp;&amp;amp; request.method === "POST") {  
  
			const { proxy } = await request.json();  
  
			if (!proxy) return new Response(JSON.stringify({ error: "پـروکـسـی وارد نشده است" }), { status: 400, headers: { "Content-Type": "application/json" } });  
  
			try {  
  
				let ip = "";  
  
				let workingProxy = proxy;  
  
				if (proxy.includes("t.me/socks") || proxy.includes("tg://socks")) {  
  
					ip = proxy.match(/server=([^&amp;amp;]+)/)?.[1] || "";  
  
				} else {  
  
					let cleanProxy = proxy.replace(/^(socks4|socks5|socks|http|https):\/\//i, "");  
  
					let remain = cleanProxy;  
  
					if (remain.includes("@")) remain = remain.substring(remain.lastIndexOf("@") + 1);  
  
					if (remain.startsWith("[")) {  
  
						ip = remain.substring(1, remain.indexOf("]"));  
  
					} else {  
  
						const lastColon = remain.lastIndexOf(":");  
  
						if (lastColon !== -1 &amp;amp;&amp;amp; remain.indexOf(":") === lastColon) ip = remain.substring(0, lastColon);  
  
						else ip = remain;  
  
					}  
  
				}  
  
				let country = "UN";  
  
				if (ip) {  
  
					try {  
  
						const geoRes = await fetc`http://ip-api.com/json/${ip}?fields=countryCode`);  
  
						const geoData = await geoRes.json();  
  
						if (geoData &amp;amp;&amp;amp; geoData.countryCode) country = geoData.countryCode;  
  
					} catch (e) {}  
  
				}  
  
				const startTime = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());  
  
				const payload = new TextEncoder().encode("GET / HTTP/1.1\r\nHost: 1.1.1.1\r\nConnection: close\r\n\r\n");  
  
				const s = await connectProxy(proxy, "1.1.1.1", 80, payload);  
  
				const reader = s.readable.getReader();  
  
				const res = await [[reader.read](http://reader.read)]([http://reader.read)()](http://reader.read)());  
  
				if (res.done || !res.value) {  
  
					s.close();  
  
					throw new Error("تایم‌اوت در دریافت دیتا");  
  
				}  
  
				s.close();  
  
				const ping = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)()) - startTime;  
  
				return new Response(JSON.stringify({ success: true, ping, country }), { headers: { "Content-Type": "application/json" } });  
  
			} catch (e) {  
  
				let msg = e.message;  
  
				if (msg.includes("Stream was cancelled") || msg.includes("network")) msg = "ارتباط با سرور قطع شد (احتمالاً پـروکـسـی مسدود یا خاموش است)";  
  
				else if (msg.includes("timeout") || msg.includes("timed out") || msg.includes("تایم‌اوت")) msg = "تایم‌اوت در اتصال (پـروکـسـی در دسترس نیست)";  
  
				else if (msg.includes("Invalid URL") || msg.includes("Invalid format")) msg = "فرمت وارد شده برای پـروکـسـی اشتباه است";  
  
				else if (msg === "err") msg = "خطای نامشخص (ارتباط برقرار نشد)";  
  
				return new Response(JSON.stringify({ error: msg }), { status: 500, headers: { "Content-Type": "application/json" } });  
  
			}  
  
		}  
  
		if (url.pathname === "/locations") {  
  
			try {  
  
				const response = await fetch("[[https://speed.cloudflare.com/locations](https://speed.cloudflare.com/locations)](https://speed.cloudflare.com/locations](https://speed.cloudflare.com/locations))", {  
  
					headers: { Referer: "[[https://speed.cloudflare.com/](https://speed.cloudflare.com/)](https://speed.cloudflare.com/](https://speed.cloudflare.com/))" },  
  
				});  
  
				const data = await response.json();  
  
				return new Response(JSON.stringify(data), {  
  
					headers: { "Content-Type": "application/json; charset=utf-8", "Access-Control-Allow-Origin": "*" },  
  
				});  
  
			} catch (e) {  
  
				return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { "Content-Type": "application/json" } });  
  
			}  
  
		}  
  
		if (url.pathname.startsWith("/api/users")) {  
  
			const pathParts = url.pathname.split("/");  
  
			const isUserAction = pathParts.length &amp;gt; 3;  
  
			if (isUserAction) {  
  
				const username = decodeURIComponent(pathParts.pop());  
  
				if (request.method === "PUT") {  
  
					const body = await request.json();  
  
					if (body.toggle_only !== undefined) {  
  
						await env.DB.prepare("UPDATE users SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END WHERE username = ?").bind(username).run();  
  
						USERS_LIST_[[CACHE.data](http://CACHE.data)]([http://CACHE.data](http://CACHE.data)) = null;  
  
						USERS_LIST_CACHE.lastFetch = 0;  
  
						return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });  
  
				} else if (body.reset_action !== undefined) {  
  
					if (body.reset_action === "volume") {  
  
						await env.DB.prepare("UPDATE users SET used_gb = 0, is_active = 1 WHERE username = ?").bind(username).run();  
  
						GLOBAL_TRAFFIC_CACHE.set(username, 0);  
  
					} else if (body.reset_action === "req") {  
  
						await env.DB.prepare("UPDATE users SET used_req = 0, is_active = 1 WHERE username = ?").bind(username).run();  
  
						USER_REQ_CACHE.set(username, 0);  
  
					} else if (body.reset_action === "time") {  
  
						await env.DB.prepare("UPDATE users SET created_at = CURRENT_TIMESTAMP, is_active = 1 WHERE username = ?").bind(username).run();  
  
					}  
  
					USERS_LIST_[[CACHE.data](http://CACHE.data)]([http://CACHE.data](http://CACHE.data)) = null;  
  
					USERS_LIST_CACHE.lastFetch = 0;  
  
						return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });  
  
					} else {  
  
						const { username: new_username, limit_gb, expiry_days, limit_req, ips, tls, port, fingerprint, ip_limit, block_porn, block_ads, frag_len, frag_int, user_proxy_iata, user_socks5, user_proxy_ip, auto_reset_vol_days, auto_reset_req_days, auto_rotate_ip, rotate_time, ip_operator, ip_count, auto_rotate_user_proxy } = body;  
  
						if (new_username &amp;amp;&amp;amp; new_username !== username) {  
  
							if (!/^[a-zA-Z0-9_-]+$/.test(new_username)) {  
  
								return new Response(JSON.stringify({ error: "نام کاربری جدید غیرمجاز است" }), { status: 400, headers: { "Content-Type": "application/json; charset=utf-8" } });  
  
							}  
  
							const existing = await env.DB.prepare("SELECT id FROM users WHERE username = ?").bind(new_username).first();  
  
							if (existing) {  
  
								return new Response(JSON.stringify({ error: "این نام کاربری از قبل وجود دارد" }), { status: 400, headers: { "Content-Type": "application/json" } });  
  
							}  
  
							if (GLOBAL_TRAFFIC_CACHE.has(username)) {  
  
								GLOBAL_TRAFFIC_CACHE.set(new_username, GLOBAL_TRAFFIC_CACHE.get(username));  
  
								GLOBAL_TRAFFIC_CACHE.delete(username);  
  
							}  
  
							if (USER_REQ_CACHE.has(username)) {  
  
								USER_REQ_CACHE.set(new_username, USER_REQ_CACHE.get(username));  
  
								USER_REQ_CACHE.delete(username);  
  
							}  
  
							if (ACTIVE_CONNECTIONS_COUNT.has(username)) {  
  
								ACTIVE_CONNECTIONS_COUNT.set(new_username, ACTIVE_CONNECTIONS_COUNT.get(username));  
  
								ACTIVE_CONNECTIONS_COUNT.delete(username);  
  
							}  
  
							if (GLOBAL_LAST_ACTIVE_WRITE.has(username)) {  
  
								GLOBAL_LAST_ACTIVE_WRITE.set(new_username, GLOBAL_LAST_ACTIVE_WRITE.get(username));  
  
								GLOBAL_LAST_ACTIVE_WRITE.delete(username);  
  
							}  
  
						}  
  
						await env.DB.prepare("UPDATE users SET username = ?, limit_gb = ?, expiry_days = ?, limit_req = ?, ips = ?, tls = ?, port = ?, fingerprint = ?, max_connections = ?, ip_limit = ?, block_porn = ?, block_ads = ?, frag_len = ?, frag_int = ?, user_proxy_iata = ?, user_socks5 = ?, user_proxy_ip = ?, auto_reset_vol_days = ?, auto_reset_req_days = ?, auto_rotate_ip = ?, rotate_time = ?, ip_operator = ?, ip_count = ?, auto_rotate_user_proxy = ? WHERE username = ?")  
  
							.bind(new_username || username, limit_gb ? parseFloat(limit_gb) : null, expiry_days ? parseInt(expiry_days) : null, limit_req ? parseInt(limit_req) : null, ips || null, tls, port, fingerprint || "chrome", ip_limit ? parseInt(ip_limit) : null, ip_limit ? parseInt(ip_limit) : null, block_porn ? 1 : 0, block_ads ? 1 : 0, frag_len !== undefined ? frag_len : "200-3000", frag_int !== undefined ? frag_int : "1-2", user_proxy_iata || null, user_socks5 || null, user_proxy_ip || null, auto_reset_vol_days ? parseInt(auto_reset_vol_days) : 0, auto_reset_req_days ? parseInt(auto_reset_req_days) : 0, auto_rotate_ip || 0, rotate_time || 0, ip_operator || "all", ip_count || 20, auto_rotate_user_proxy ? 1 : 0, username)  
  
							.run();  
  
					USERS_LIST_[[CACHE.data](http://CACHE.data)]([http://CACHE.data](http://CACHE.data)) = null;  
  
					USERS_LIST_CACHE.lastFetch = 0;  
  
					return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });  
  
				}  
  
			}  
  
			if (request.method === "DELETE") {  
  
				await env.DB.prepare("DELETE FROM users WHERE username = ?").bind(username).run();  
  
				USERS_LIST_[[CACHE.data](http://CACHE.data)]([http://CACHE.data](http://CACHE.data)) = null;  
  
				USERS_LIST_CACHE.lastFetch = 0;  
  
				return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });  
  
				}  
  
		} else {  
  
			if (request.method === "GET") {  
  
				const now = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());  
  
				if (USERS_LIST_[[CACHE.data](http://CACHE.data)]([http://CACHE.data](http://CACHE.data)) &amp;amp;&amp;amp; (now - USERS_LIST_CACHE.lastFetch &amp;lt; 30000)) {  
  
					return new Response(USERS_LIST_[[CACHE.data](http://CACHE.data)]([http://CACHE.data](http://CACHE.data)), {  
  
						headers: {  
  
							"Content-Type": "application/json; charset=utf-8",  
  
							"Cache-Control": "max-age=30"  
  
						},  
  
					});  
  
				}  
  
				try {  
  
					await flushExpiredTraffic(env);  
  
				} catch (e) {}  
  
				try {  
  
					const { results } = await env.DB.prepare("SELECT  *FROM users ORDER BY id DESC").all();*  
  
					*const enrichedUsers = (results || []).map((user) =&amp;gt; ({*  
  
						*...user,*  
  
						*is_online: user.last_active &amp;amp;&amp;amp; now - user.last_active &amp;lt; 65000 ? 1 : 0,*  
  
						*online_count: getActiveIpCount(user.active_ips),*  
  
					*}));*  
  
						*let cfReqs = { today: 0, total: 0 };*  
  
						*try {*  
  
							*const liveCf = await getCfUsage(env);*  
  
							*const todayStr = new Date().toISOString().split("T")[0];*  
  
							*const dateRow = await env.DB.prepare("SELECT value FROM settings WHERE key = 'req_last_date'").first();*  
  
							*const totalRow = await env.DB.prepare("SELECT value FROM settings WHERE key = 'req_total'").first();*  
  
							*let dbTotal = totalRow ? parseInt(totalRow.value) || 0 : 0;*  
  
							*let dbToday = 0;*  
  
							*if (dateRow &amp;amp;&amp;amp; dateRow.value === todayStr) {*  
  
								*const todayRow = await env.DB.prepare("SELECT value FROM settings WHERE key = 'req_today'").first();*  
  
								*dbToday = todayRow ? parseInt(todayRow.value) || 0 : 0;*  
  
							*}*  
  
							*if ([[liveCf.today](http://liveCf.today)]([http://liveCf.today](http://liveCf.today)) &amp;gt; dbToday) {*  
  
								*dbToday = [[liveCf.today](http://liveCf.today)]([http://liveCf.today](http://liveCf.today));*  
  
								*await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_today', ?) ON CONFLICT(key) DO UPDATE SET value = ?").bind(String(dbToday), String(dbToday)).run();*  
  
								*await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_last_date', ?) ON CONFLICT(key) DO UPDATE SET value = ?").bind(todayStr, todayStr).run();*  
  
							*}*  
  
							*if ([[liveCf.total](http://liveCf.total)]([http://liveCf.total](http://liveCf.total)) &amp;gt; dbTotal) {*  
  
								*dbTotal = [[liveCf.total](http://liveCf.total)]([http://liveCf.total](http://liveCf.total));*  
  
								*await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_total', ?) ON CONFLICT(key) DO UPDATE SET value = ?").bind(String(dbTotal), String(dbTotal)).run();*  
  
							*}*  
  
						*[[cfReqs.today](http://cfReqs.today)]([http://cfReqs.today](http://cfReqs.today)) = dbToday + GLOBAL_REQ_COUNT;*  
  
						*[[cfReqs.total](http://cfReqs.total)]([http://cfReqs.total](http://cfReqs.total)) = dbTotal + GLOBAL_REQ_COUNT;*  
  
					*} catch (e) {}*  
  
					*const finalResponseData = JSON.stringify({*  
  
						*users: enrichedUsers,*  
  
						*serverTime: now,*  
  
						*cfRequestsToday: [[cfReqs.today](http://cfReqs.today)]([http://cfReqs.today](http://cfReqs.today)),*  
  
						*cfRequestsTotal: [[cfReqs.total](http://cfReqs.total)]([http://cfReqs.total](http://cfReqs.total)),*  
  
					*});*  
  
					*USERS_LIST_[[CACHE.data](http://CACHE.data)]([http://CACHE.data](http://CACHE.data)) = finalResponseData;*  
  
					*USERS_LIST_CACHE.lastFetch = now;*  
  
					*return new Response(finalResponseData, {*  
  
						*headers: {*  
  
							*"Content-Type": "application/json; charset=utf-8",*  
  
							*"Cache-Control": "max-age=30",*  
  
						*},*  
  
					*});*  
  
					*} catch (dbErr) {*  
  
						*return new Response(*  
  
							*JSON.stringify({*  
  
								*users: [],*  
  
								*serverTime: [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)()),*  
  
								*cfRequestsToday: 0,*  
  
								*cfRequestsTotal: 0,*  
  
								*error: dbErr.message*  
  
							*}),*  
  
							*{*  
  
								*status: 200, *  
  
								*headers: {*  
  
									*"Content-Type": "application/json",*  
  
									*"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",*  
  
								*},*  
  
							*}*  
  
						*);*  
  
					*}*  
  
				*}*  
  
				*if (request.method === "POST") {*  
  
					*const { username, uuid, limit_gb, expiry_days, limit_req, ips, tls, port, fingerprint, ip_limit, used_gb, used_req, created_at, is_active, block_porn, block_ads, frag_len, frag_int, user_proxy_iata, user_socks5, user_proxy_ip, auto_reset_vol_days, auto_reset_req_days, auto_rotate_ip, rotate_time, ip_operator, ip_count, auto_rotate_user_proxy } = await request.json();*  
  
					*if (!username) {*  
  
						*return new Response(JSON.stringify({ error: "نام کاربری اجباری است" }), { status: 400, headers: { "Content-Type": "application/json" } });*  
  
					*}*  
  
					*if (username.length &amp;gt; 32) {*  
  
						*return new Response(JSON.stringify({ error: "نام کاربری نمی‌تواند بیشتر از ۳۲ کاراکتر باشد" }), { status: 400, headers: { "Content-Type": "application/json" } });*  
  
					*}*  
  
					*if (!/^[a-zA-Z0-9_-]+$/.test(username)) {*  
  
						*return new Response(JSON.stringify({ error: "نام کاربری غیرمجاز است (فقط حروف، اعداد، خط تیره و آندرلاین)" }), { status: 400, headers: { "Content-Type": "application/json; charset=utf-8" } });*  
  
					*}*  
  
					*let finalUuid = uuid;*  
  
					*if (!finalUuid) {*  
  
						*const randomHex = Array.from(crypto.getRandomValues(new Uint8Array(6)))*  
  
							*.map(b =&amp;gt; b.toString(16).padStart(2, "0"))*  
  
							*.join("");*  
  
						*finalUuid =* `50414e45-4c5f-5a45-5553-${randomHex}`*;*  
  
					*}*  
  
					*const parsedUsedGb = parseFloat(used_gb);*  
  
					*const finalUsedGb = !isNaN(parsedUsedGb) ? parsedUsedGb : 0;*  
  
					*const parsedUsedReq = parseInt(used_req);*  
  
					*const finalUsedReq = !isNaN(parsedUsedReq) ? parsedUsedReq : 0;*  
  
					*const finalCreatedAt = created_at || new Date().toISOString();*  
  
					*const parsedIsActive = parseInt(is_active);*  
  
					*const finalIsActive = !isNaN(parsedIsActive) ? parsedIsActive : 1;*  
  
					*const existingUser = await env.DB.prepare("SELECT id FROM users WHERE username = ?").bind(username).first();*  
  
					*if (existingUser) {*  
  
						*return new Response(JSON.stringify({ error: "این نام کاربری از قبل وجود دارد" }), { status: 400, headers: { "Content-Type": "application/json; charset=utf-8" } });*  
  
					*}*  
  
				*try {*  
  
					*const todayUtc = Math.floor([[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)()) / 86400000)*  86400000;  
  
					const nowTime = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());  
  
					await env.DB.prepare("INSERT INTO users (username, uuid, limit_gb, expiry_days, limit_req, ips, connection_type, tls, port, fingerprint, max_connections, ip_limit, used_gb, used_req, created_at, is_active, block_porn, block_ads, frag_len, frag_int, user_proxy_iata, user_socks5, user_proxy_ip, auto_reset_vol_days, auto_reset_req_days, last_reset_vol_time, last_reset_req_time, auto_rotate_ip, rotate_time, ip_operator, ip_count, last_rotate_time, auto_rotate_user_proxy) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")  
  
						.bind(username, finalUuid, limit_gb ? parseFloat(limit_gb) : null, expiry_days ? parseInt(expiry_days) : null, limit_req ? parseInt(limit_req) : null, ips || null, "vl" + "e" + "ss", tls, port, fingerprint || "chrome", ip_limit ? parseInt(ip_limit) : null, ip_limit ? parseInt(ip_limit) : null, finalUsedGb, finalUsedReq, finalCreatedAt, finalIsActive, block_porn ? 1 : 0, block_ads ? 1 : 0, frag_len !== undefined ? frag_len : "200-3000", frag_int !== undefined ? frag_int : "1-2", user_proxy_iata || null, user_socks5 || null, user_proxy_ip || null, auto_reset_vol_days ? parseInt(auto_reset_vol_days) : 0, auto_reset_req_days ? parseInt(auto_reset_req_days) : 0, todayUtc, todayUtc, auto_rotate_ip || 0, rotate_time || 0, ip_operator || "all", ip_count || 20, nowTime, auto_rotate_user_proxy ? 1 : 0)  
  
						.run();  
  
					USERS_LIST_[[CACHE.data](http://CACHE.data)]([http://CACHE.data](http://CACHE.data)) = null;  
  
					USERS_LIST_CACHE.lastFetch = 0;  
  
					return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });  
  
					} catch (err) {  
  
						return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { "Content-Type": "application/json" } });  
  
					}  
  
				}  
  
			}  
  
		}  
  
		return new Response(JSON.stringify({ error: "Not Found" }), { status: 404, headers: { "Content-Type": "application/json" } });  
  
	},  
  
};  
  
let schemaEnsured = false;  
  
let schemaPromise = null;  
  
let cachedPanelPassword = null;  
  
const DbService = {  
  
	async ensureSchema(db) {  
  
		if (schemaEnsured) return;  
  
		if (schemaPromise) {  
  
			await schemaPromise;  
  
			return;  
  
		}  
  
		schemaPromise = (async () =&amp;gt; {  
  
			try {  
  
				await db.prepar`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, uuid TEXT, limit_gb REAL, expiry_days INTEGER, ips TEXT, connection_type TEXT, tls TEXT, port INTEGER, used_gb REAL DEFAULT 0, is_active INTEGER DEFAULT 1, last_active INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`).run();  
  
			} catch (e) {}  
  
			try {  
  
				await db.prepare("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)").run();  
  
			} catch (e) {}  
  
			try {  
  
				const { results } = await db.prepare("PRAGMA table_info(users)").all();  
  
				const existingCols = new Set((results || []).map((r) =&amp;gt; [[r.name](http://r.name)]([http://r.name](http://r.name))));  
  
				const colsToAdd = [  
  
					{ name: "is_active", def: "INTEGER DEFAULT 1" },  
  
					{ name: "last_active", def: "INTEGER" },  
  
					{ name: "fingerprint", def: "TEXT DEFAULT 'chrome'" },  
  
					{ name: "max_connections", def: "INTEGER" },  
  
					{ name: "limit_req", def: "INTEGER" },  
  
					{ name: "used_req", def: "INTEGER DEFAULT 0" },  
  
					{ name: "ip_limit", def: "INTEGER DEFAULT NULL" },  
  
					{ name: "active_ips", def: "TEXT DEFAULT NULL" },  
  
					{ name: "block_porn", def: "INTEGER DEFAULT 0" },  
  
					{ name: "block_ads", def: "INTEGER DEFAULT 0" },  
  
					{ name: "frag_len", def: "TEXT DEFAULT '200-3000'" },  
  
					{ name: "frag_int", def: "TEXT DEFAULT '1-2'" },  
  
					{ name: "lifetime_used_gb", def: "REAL DEFAULT 0" },  
  
					{ name: "user_proxy_ip", def: "TEXT DEFAULT NULL" },  
  
					{ name: "user_proxy_iata", def: "TEXT DEFAULT NULL" },  
  
					{ name: "user_socks5", def: "TEXT DEFAULT NULL" },  
  
					{ name: "auto_reset_vol_days", def: "INTEGER DEFAULT 0" },  
  
					{ name: "auto_reset_req_days", def: "INTEGER DEFAULT 0" },  
  
					{ name: "last_reset_vol_time", def: "INTEGER DEFAULT 0" },  
  
					{ name: "last_reset_req_time", def: "INTEGER DEFAULT 0" },  
  
					{ name: "auto_rotate_ip", def: "INTEGER DEFAULT 0" },  
  
					{ name: "rotate_time", def: "INTEGER DEFAULT 0" },  
  
					{ name: "ip_operator", def: "TEXT DEFAULT 'all'" },  
  
					{ name: "ip_count", def: "INTEGER DEFAULT 20" },  
  
					{ name: "last_rotate_time", def: "INTEGER DEFAULT 0" },  
  
					{ name: "auto_rotate_user_proxy", def: "INTEGER DEFAULT 0" }  
  
				];  
  
				const stmts = [];  
  
				for (const col of colsToAdd) {  
  
					if (!existingCols.has([[col.name](http://col.name)]([http://col.name](http://col.name)))) {  
  
						stmts.push(db.prepar`ALTER TABLE users ADD COLUMN ${col.name} ${col.def}`));  
  
					}  
  
				}  
  
				if (stmts.length &amp;gt; 0) {  
  
					await db.batch(stmts);  
  
				}  
  
			} catch (e) {}  
  
			try {  
  
				await db.prepare("UPDATE users SET ip_limit = max_connections WHERE ip_limit IS NULL AND max_connections IS NOT NULL").run();  
  
			} catch (e) {}  
  
			try {  
  
				await db.prepare("UPDATE users SET lifetime_used_gb = used_gb WHERE lifetime_used_gb = 0 OR lifetime_used_gb IS NULL").run();  
  
			} catch (e) {}  
  
		})();  
  
		await schemaPromise;  
  
		schemaEnsured = true;  
  
	},  
  
	async getPanelPassword(db) {  
  
		if (cachedPanelPassword !== null) return cachedPanelPassword;  
  
		try {  
  
			const row = await db.prepare("SELECT value FROM settings WHERE key = 'panel_password'").first();  
  
			cachedPanelPassword = row ? row.value : "";  
  
			return cachedPanelPassword || null;  
  
		} catch (e) {  
  
			return null;  
  
		}  
  
	},  
  
	async setPanelPassword(db, password) {  
  
		await db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('panel_password', ?)").bind(password).run();  
  
		cachedPanelPassword = password;  
  
	},  
  
	async verifyApiAuth(request, env) {  
  
		const storedPasswordHash = await this.getPanelPassword(env.DB);  
  
		if (!storedPasswordHash) return true;  
  
		const cookies = request.headers.get("Cookie") || "";  
  
		const sessionCookie = cookies.split(";").find((c) =&amp;gt; c.trim().startsWith("panel_session="));  
  
		if (!sessionCookie) return false;  
  
		const sessionToken = sessionCookie.split("=")[1].trim();  
  
		return sessionToken === storedPasswordHash;  
  
	},  
  
	async sha256(message) {  
  
		const salt = "Ma_Ke_Vaslim_SECURE_SALT_2026";  
  
		const msgBuffer = new TextEncoder().encode(message + salt);  
  
		const hashBuffer = await crypto.subtle.digest("SHA-256", msgBuffer);  
  
		const hashArray = Array.from(new Uint8Array(hashBuffer));  
  
		return [[hashArray.map](http://hashArray.map)]([http://hashArray.map)((b](http://hashArray.map)((b)) =&amp;gt; b.toString(16).padStart(2, "0")).join("");  
  
	},  
  
	async oldSha256(message) {  
  
		const msgBuffer = new TextEncoder().encode(message);  
  
		const hashBuffer = await crypto.subtle.digest("SHA-256", msgBuffer);  
  
		const hashArray = Array.from(new Uint8Array(hashBuffer));  
  
		return [[hashArray.map](http://hashArray.map)]([http://hashArray.map)((b](http://hashArray.map)((b)) =&amp;gt; b.toString(16).padStart(2, "0")).join("");  
  
	},  
  
};  
  
function getActiveIpCount(activeIpsJson) {  
  
	if (!activeIpsJson) return 0;  
  
	try {  
  
		const activeIps = JSON.parse(activeIpsJson);  
  
		const now = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());  
  
		let count = 0;  
  
		for (const [ip, data] of Object.entries(activeIps)) {  
  
			const lastSeen = data &amp;amp;&amp;amp; typeof data === "object" ? data.timestamp : data;  
  
			if (now - lastSeen &amp;lt;= 20000) {  
  
				count++;  
  
			}  
  
		}  
  
		return count;  
  
	} catch (e) {  
  
		return 0;  
  
	}  
  
}  
  
let CACHED_CF_LOCATIONS = null;  
  
let CACHED_CF_LOCATIONS_TIME = 0;  
  
const SubscriptionService = {  
  
	async generateText(user, host) {  
  
		let remVol = "∞";  
  
		if (user.limit_gb) {  
  
			let rem = user.limit_gb - (user.used_gb || 0);  
  
			remVol = rem &amp;gt; 0 ? rem.toFixed(1) + "GB" : "0GB";  
  
		}  
  
		let remTime = "∞";  
  
		if (user.expiry_days &amp;amp;&amp;amp; user.created_at) {  
  
			const created = new Date(user.created_at);  
  
			const expiryDate = new Date(created.getTime() + user.expiry_days  *24*  60  *60*  1000);  
  
			const diffDays = Math.ceil((expiryDate.getTime() - [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)())) / (1000  *60*  60  *24));*  
  
			*remTime = diffDays &amp;gt; 0 ? diffDays + "D" : "0D";*  
  
		*}*  
  
		*let remReq = "∞";*  
  
		*if (user.limit_req) {*  
  
			*let rem = user.limit_req - (user.used_req || 0);*  
  
			*remReq = rem &amp;gt; 0 ? rem.toLocaleString() + "Req" : "0Req";*  
  
		*}*  
  
		*const info =* `[${remVol}|${remTime}|${remReq}]`*;*  
  
		*const fp = user.fingerprint || "chrome";*  
  
		*const links = [];*  
  
		*const rawIpList = user.ips*  
  
			*? user.ips.split("\n").map((ip) =&amp;gt; ip.trim()).filter((ip) =&amp;gt; ip.length &amp;gt; 0)*  
  
			*: [host];*  
  
		*function encodeProxyForPath(proxyStr) {*  
  
			*if (!proxyStr) return null;*  
  
			*try {*  
  
				*return btoa(encodeURIComponent(proxyStr)).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');*  
  
			*} catch (e) { return null; }*  
  
		*}*  
  
		*function generateConfig(uuid, cleanIp, port, configName, fingerprint, fragStr, extraStr, proxyPath) {*  
  
			*const encodedName = encodeURIComponent(configName);*  
  
			*const sniDomain = MY_SECRET_DOMAIN || host;*  
  
			*const wsPath = proxyPath ?* `/p/${proxyPath}/Ma_Ke_Vaslim` *:* `/Ma_Ke_Vaslim`*;*  
  
			*const params = new URLSearchParams({*  
  
				*encryption: "none",*  
  
				*security: "tls",*  
  
				*sni: sniDomain,*  
  
				*host: sniDomain,*  
  
				*fp: fingerprint,*  
  
				*type: "ws",*  
  
				*path: wsPath,*  
  
				*alpn: "h2,http/1.1"*  
  
			*});*  
  
			*let link =* `vless://${uuid}@${cleanIp}:${port}?${params.toString()}${fragStr}`*;*  
  
			*if (extraStr) link +=* `&extraParams=${extraStr}`*;*  
  
			*link +=* `#${encodedName}`*;*  
  
			*return link;*  
  
		*}*  
  
		*rawIpList.forEach((item) =&amp;gt; {*  
  
			*let ipOrDomain, port, customName, perIpProxy;*  
  
			*if (item.includes("|")) {*  
  
				*const [mainPart, proxyPart] = item.split("|");*  
  
				*perIpProxy = proxyPart.trim();*  
  
				*item = mainPart.trim();*  
  
			*}*  
  
			*if (item.includes("#")) {*  
  
				*const [addressPart, name] = item.split("#");*  
  
				*customName = name || user.username;*  
  
				*if (addressPart.includes(":")) {*  
  
					*[ipOrDomain, port] = addressPart.split(":");*  
  
				*} else {*  
  
					*ipOrDomain = addressPart;*  
  
					*port = "443";*  
  
				*}*  
  
			*} else if (item.includes(":")) {*  
  
				*const parts = item.split(":");*  
  
				*ipOrDomain = parts[0];*  
  
				*port = parts[1] || "443";*  
  
				*customName = user.username;*  
  
			*} else {*  
  
				*ipOrDomain = item;*  
  
				*port = "443";*  
  
				*customName = user.username;*  
  
			*}*  
  
			*if (!ipOrDomain || !port) return;*  
  
			*const remark =* `${customName} ${info}`*;*  
  
			*const lowerName = (customName || "").toLowerCase();*  
  
			*let currentFragment = "&amp;amp;fragment=100-200,10-20";*  
  
			*let extraParamsObj = {*  
  
				*"mode": "auto",*  
  
				*"xPaddingBytes": "100-1000",*  
  
				*"xmux": { "maxConcurrency": "4-8" }*  
  
			*};*  
  
			*if (lowerName.includes("mci") || lowerName.includes("همراه")) {*  
  
				*currentFragment = "&amp;amp;fragment=10-20,10-20";*  
  
				*extraParamsObj.xPaddingBytes = "100-500";*  
  
				*extraParamsObj.xmux.maxConcurrency = "4-8";*  
  
			*}*  
  
			*else if (lowerName.includes("irancell") || lowerName.includes("ایرانسل")) {*  
  
				*currentFragment = "&amp;amp;fragment=100-200,10-20";*  
  
				*extraParamsObj.xPaddingBytes = "500-1500";*  
  
				*extraParamsObj.xmux.maxConcurrency = "4-8";*  
  
			*}*  
  
			*else if (lowerName.includes("gaming") || lowerName.includes("گیم")) {*  
  
				*currentFragment = "";*  
  
				*extraParamsObj.xmux.maxConcurrency = "1-4";*  
  
			*}*  
  
			*const extraParams = encodeURIComponent(JSON.stringify(extraParamsObj));*  
  
			*const proxyB64 = encodeProxyForPath(perIpProxy || user.user_socks5 || null);*  
  
			*if (proxyB64) {*  
  
				*const proxyRemark =* `${customName}🔒${info}`*;*  
  
				*const proxyLink = generateConfig(user.uuid, ipOrDomain, port, proxyRemark, fp, currentFragment, extraParams, proxyB64);*  
  
				*links.push(proxyLink);*  
  
			*}*  
  
			*const directLink = generateConfig(user.uuid, ipOrDomain, port, remark, fp, currentFragment, extraParams, null);*  
  
			*links.push(directLink);*  
  
            *const masterUuid = "e5b8a6a1-a7b3-4f16-89d2-97b7914db459";*  
  
			*const railwayRemark =* `${customName}🛰️ریلوی-->🇳🇱 ${info}`*;*  
  
			*const railwayLink = generateConfig(masterUuid, ipOrDomain, port, railwayRemark, fp, currentFragment, extraParams, null);*  
  
			*const railwayPath =* `/railway-io/e5b8a6a1-a7b3-4f16-89d2-97b7914db459`*;*  
  
			*const railwayParams = new URLSearchParams({*  
  
				*encryption: "none",*  
  
				*security: "tls",*  
  
				*sni: MY_SECRET_DOMAIN || host,*  
  
				*host: MY_SECRET_DOMAIN || host,*  
  
				*fp: fp,*  
  
				*type: "ws",*  
  
				*path: railwayPath,*  
  
				*alpn: "h2,http/1.1"*  
  
			*});*  
  
			*let rLink =* `vless://${masterUuid}@${ipOrDomain}:${port}?${railwayParams.toString()}${currentFragment}`*;*  
  
			*if (extraParams) rLink +=* `&extraParams=${extraParams}`*;*  
  
			*rLink +=* `#${encodeURIComponent(railwayRemark)}`*;*  
  
			*links.push(rLink);*  
  
		*});*  
  
		*const subContent = btoa(unescape(encodeURIComponent(links.join("\n"))));*  
  
		*const downloadBytes = Math.floor((user.used_gb || 0)*  1073741824);  
  
		const totalBytes = user.limit_gb ? Math.floor(user.limit_gb  *1073741824) : 0;*  
  
		*let expireTimestamp = 0;*  
  
		*if (user.expiry_days &amp;amp;&amp;amp; user.created_at) {*  
  
			*expireTimestamp = Math.floor((new Date(user.created_at).getTime() + user.expiry_days*  86400000) / 1000);  
  
		}  
  
		const subUserInfo = `upload=0; download=${downloadBytes}; total=${totalBytes}; expire=${expireTimestamp}`;  
  
		return new Response(subContent, {  
  
			headers: {  
  
				"Content-Type": "text/plain; charset=utf-8",  
  
				"Access-Control-Allow-Origin": "*",  
  
				"Cache-Control": "no-store",  
  
				"Subscription-Userinfo": subUserInfo,  
  
			},  
  
		});  
  
	},  
  
};  
  
async function flushExpiredTraffic(env) {  
  
	const now = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());  
  
	for (const [key, val] of DNS_CACHE.entries()) {  
  
		if (now &amp;gt; val.expires) DNS_CACHE.delete(key);  
  
	}  
  
	for (const [ip, record] of LOGIN_ATTEMPTS.entries()) {  
  
		if (now - record.lastAttempt &amp;gt; 900000) LOGIN_ATTEMPTS.delete(ip);  
  
	}  
  
	const allUsers = new Set([...GLOBAL_TRAFFIC_CACHE.keys(), ...USER_REQ_CACHE.keys()]);  
  
	for (const uname of allUsers) {  
  
		const cachedBytes = GLOBAL_TRAFFIC_CACHE.get(uname) || 0;  
  
		const cachedReqs = USER_REQ_CACHE.get(uname) || 0;  
  
		const activeCount = ACTIVE_CONNECTIONS_COUNT.get(uname) || 0;  
  
		if (cachedBytes &amp;lt;= 0 &amp;amp;&amp;amp; cachedReqs &amp;lt;= 0) {  
  
			GLOBAL_TRAFFIC_CACHE.delete(uname);  
  
			USER_REQ_CACHE.delete(uname);  
  
			if (activeCount &amp;lt;= 0) {  
  
				GLOBAL_LAST_ACTIVE_WRITE.delete(uname);  
  
				GLOBAL_LAST_ACTIVE_WRITE.delete(uname + "_hb");  
  
			}  
  
			continue;  
  
		}  
  
		if (GLOBAL_WRITE_LOCK.get(uname)) continue;  
  
		const lastActive = GLOBAL_LAST_ACTIVE_WRITE.get(uname) || 0;  
  
		if (activeCount &amp;lt;= 0 || now - lastActive &amp;gt; 20000) {  
  
			GLOBAL_WRITE_LOCK.set(uname, true);  
  
			GLOBAL_TRAFFIC_CACHE.set(uname, 0);  
  
			USER_REQ_CACHE.set(uname, 0);  
  
			const deltaGb = cachedBytes / (1024  *1024*  1024);  
  
			try {  
  
				await env.DB.prepare("UPDATE users SET used_gb = used_gb + ?, lifetime_used_gb = lifetime_used_gb + ?, used_req = used_req + ? WHERE username = ?").bind(deltaGb, deltaGb, cachedReqs, uname).run();  
  
			} catch (e) {  
  
				console.error(e.message);  
  
			} finally {  
  
				GLOBAL_WRITE_LOCK.delete(uname);  
  
				if (activeCount &amp;lt;= 0) {  
  
					GLOBAL_LAST_ACTIVE_WRITE.delete(uname);  
  
					GLOBAL_LAST_ACTIVE_WRITE.delete(uname + "_hb");  
  
				}  
  
			}  
  
		}  
  
	}  
  
}  
  
async function handleVLESS(env, storedData = null, ctx = null, request = null) {  
  
	const clientIP = request ? request.headers.get("CF-Connecting-IP") || "unknown" : "unknown";  
  
	const socketPair = new WebSocketPair();  
  
	const [clientSock, serverSock] = Object.values(socketPair);  
  
	serverSock.accept();  
  
	serverSock.binaryType = "arraybuffer";  
  
	let username = null;  
  
	let validUUID = null;  
  
	let targetDns = "8.8.4.4";  
  
	let targetDoh = "[[https://cloudflare-dns.com/dns-query](https://cloudflare-dns.com/dns-query)](https://cloudflare-dns.com/dns-query](https://cloudflare-dns.com/dns-query))";  
  
	function addBytes(bytes) {  
  
		if (bytes &amp;lt;= 0) return;  
  
		if (!username) {  
  
			uncountedBytes += bytes;  
  
			return;  
  
		}  
  
		if (uncountedBytes &amp;gt; 0) {  
  
			bytes += uncountedBytes;  
  
			uncountedBytes = 0;  
  
		}  
  
		let current = GLOBAL_TRAFFIC_CACHE.get(username) || 0;  
  
		GLOBAL_TRAFFIC_CACHE.set(username, current + bytes);  
  
		GLOBAL_LAST_ACTIVE_WRITE.set(username, [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)()));  
  
		if (GLOBAL_WRITE_LOCK.get(username)) return;  
  
		let lastDbWrite = GLOBAL_LAST_DB_WRITE.get(username) || 0;  
  
		let now = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());  
  
		let thresholdBytes = 50  *1024*  1024;  
  
		if ((current &amp;gt;= thresholdBytes &amp;amp;&amp;amp; now - lastDbWrite &amp;gt; 10000) || (current &amp;gt; 0 &amp;amp;&amp;amp; now - lastDbWrite &amp;gt; 60000)) {  
  
			GLOBAL_WRITE_LOCK.set(username, true);  
  
			let toCommit = GLOBAL_TRAFFIC_CACHE.get(username) || 0;  
  
			let toCommitReq = USER_REQ_CACHE.get(username) || 0;  
  
			if (toCommit &amp;lt;= 0 &amp;amp;&amp;amp; toCommitReq &amp;lt;= 0) {  
  
				GLOBAL_WRITE_LOCK.set(username, false);  
  
				return;  
  
			}  
  
			GLOBAL_TRAFFIC_CACHE.set(username, (GLOBAL_TRAFFIC_CACHE.get(username) || 0) - toCommit);  
  
			USER_REQ_CACHE.set(username, (USER_REQ_CACHE.get(username) || 0) - toCommitReq);  
  
			GLOBAL_LAST_DB_WRITE.set(username, now);  
  
			let deltaGb = toCommit / (1024  *1024*  1024);  
  
			let writeTask = async () =&amp;gt; {  
  
				try {  
  
					await env.DB.prepare("UPDATE users SET used_gb = used_gb + ?, lifetime_used_gb = lifetime_used_gb + ?, used_req = used_req + ? WHERE username = ?").bind(deltaGb, deltaGb, toCommitReq, username).run();  
  
				} catch (e) {  
  
					console.error(e.message);  
  
					GLOBAL_TRAFFIC_CACHE.set(username, (GLOBAL_TRAFFIC_CACHE.get(username) || 0) + toCommit);  
  
					USER_REQ_CACHE.set(username, (USER_REQ_CACHE.get(username) || 0) + toCommitReq);  
  
				} finally {  
  
					GLOBAL_WRITE_LOCK.set(username, false);  
  
				}  
  
			};  
  
			if (ctx) ctx.waitUntil(writeTask());  
  
			else writeTask();  
  
		}  
  
	}  
  
	let isOfflineSet = false;  
  
	let hasCountedAsActive = false;  
  
	const setOffline = () =&amp;gt; {  
  
		if (isOfflineSet) return;  
  
		isOfflineSet = true;  
  
		const uname = username;  
  
		if (!uname) return;  
  
		if (clientIP &amp;amp;&amp;amp; clientIP !== "unknown" &amp;amp;&amp;amp; validUUID) {  
  
			const removeIpTask = async () =&amp;gt; {  
  
				try {  
  
					const user = await env.DB.prepare("SELECT active_ips FROM users WHERE uuid = ?").bind(validUUID).first();  
  
					if (user) {  
  
						console.lo`[setOffline Task] DB active_ips for ${uname}: ${user.active_ips}`);  
  
						let activeIps = JSON.parse(user.active_ips || "{}");  
  
						if (activeIps[clientIP]) {  
  
							if (typeof activeIps[clientIP] === "object") {  
  
								activeIps[clientIP].count = (activeIps[clientIP].count || 1) - 1;  
  
								if (activeIps[clientIP].count &amp;lt;= 0) {  
  
									delete activeIps[clientIP];  
  
								}  
  
							} else {  
  
								delete activeIps[clientIP];  
  
							}  
  
							await env.DB.prepare("UPDATE users SET active_ips = ? WHERE uuid = ?").bind(JSON.stringify(activeIps), validUUID).run();  
  
							console.lo`[setOffline Task] Updated active_ips in DB to: ${JSON.stringify(activeIps)}`);  
  
						} else {  
  
							console.lo`[setOffline Task] IP ${clientIP} not found in user's active_ips`);  
  
						}  
  
					}  
  
				} catch (e) {  
  
					console.erro`[setOffline Task] Error: ${e.message}`);  
  
				}  
  
			};  
  
			if (ctx) ctx.waitUntil(removeIpTask());  
  
			else removeIpTask();  
  
		}  
  
		let activeCount = ACTIVE_CONNECTIONS_COUNT.get(uname) || 0;  
  
		if (hasCountedAsActive) {  
  
			activeCount = Math.max(0, activeCount - 1);  
  
		}  
  
		if (activeCount &amp;lt;= 0) {  
  
			ACTIVE_CONNECTIONS_COUNT.delete(uname);  
  
			let cachedBytes = GLOBAL_TRAFFIC_CACHE.get(uname) || 0;  
  
			let cachedReqs = USER_REQ_CACHE.get(uname) || 0;  
  
			if ((cachedBytes &amp;gt; 0 || cachedReqs &amp;gt; 0) &amp;amp;&amp;amp; !GLOBAL_WRITE_LOCK.get(uname)) {  
  
				GLOBAL_WRITE_LOCK.set(uname, true);  
  
				GLOBAL_TRAFFIC_CACHE.set(uname, (GLOBAL_TRAFFIC_CACHE.get(uname) || 0) - cachedBytes);  
  
				USER_REQ_CACHE.set(uname, (USER_REQ_CACHE.get(uname) || 0) - cachedReqs);  
  
				const deltaGb = cachedBytes / (1024  *1024*  1024);  
  
				const writeTask = async () =&amp;gt; {  
  
					try {  
  
						await env.DB.prepare("UPDATE users SET used_gb = used_gb + ?, lifetime_used_gb = lifetime_used_gb + ?, used_req = used_req + ? WHERE username = ?").bind(deltaGb, deltaGb, cachedReqs, uname).run();  
  
					} catch (e) {  
  
						console.error(e.message);  
  
						GLOBAL_TRAFFIC_CACHE.set(uname, (GLOBAL_TRAFFIC_CACHE.get(uname) || 0) + cachedBytes);  
  
						USER_REQ_CACHE.set(uname, (USER_REQ_CACHE.get(uname) || 0) + cachedReqs);  
  
					} finally {  
  
						GLOBAL_WRITE_LOCK.delete(uname);  
  
						GLOBAL_LAST_ACTIVE_WRITE.delete(uname);  
  
					}  
  
				};  
  
				if (ctx) {  
  
					ctx.waitUntil(writeTask());  
  
				} else {  
  
					writeTask();  
  
				}  
  
			} else {  
  
				GLOBAL_LAST_ACTIVE_WRITE.delete(uname);  
  
			}  
  
		} else {  
  
			ACTIVE_CONNECTIONS_COUNT.set(uname, activeCount);  
  
		}  
  
	};  
  
	let heartbeat;  
  
	const runHeartbeat = async () =&amp;gt; {  
  
		if (serverSock.readyState === [[WebSocket.OPEN](http://WebSocket.OPEN)]([http://WebSocket.OPEN](http://WebSocket.OPEN))) {  
  
			try {  
  
				serverSock.send(new Uint8Array(0));  
  
				if (!validUUID || !username) {  
  
					heartbeat = setTimeout(runHeartbeat, Math.floor(Math.random()  *5000) + 20000);*  
  
					*return;*  
  
				*}*  
  
				*const nowTime = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());*  
  
				*const lastCheck = GLOBAL_LAST_ACTIVE_WRITE.get(username + "_hb") || 0;*  
  
				*if (nowTime - lastCheck &amp;gt;= 20000) {*  
  
					*GLOBAL_LAST_ACTIVE_WRITE.set(username + "_hb", nowTime);*  
  
					*const user = await env.DB.prepare("SELECT is_active, limit_gb, used_gb, limit_req, used_req, expiry_days, created_at, ip_limit, active_ips FROM users WHERE uuid = ?").bind(validUUID).first();*  
  
					*let isExpired = false;*  
  
					*let isIpLimitExpired = false;*  
  
					*let updatedActiveIps = null;*  
  
					*if (!user || [[user.is](http://user.is)]([http://user.is)_active](http://user.is)_active) === 0) {*  
  
						*isExpired = true;*  
  
					*} else {*  
  
						*if (user.limit_gb &amp;amp;&amp;amp; user.used_gb &amp;gt;= user.limit_gb) isExpired = true;*  
  
						*if (user.limit_req &amp;amp;&amp;amp; user.used_req + (USER_REQ_CACHE.get(username) || 0) &amp;gt;= user.limit_req) isExpired = true;*  
  
						*if (user.expiry_days &amp;amp;&amp;amp; user.created_at) {*  
  
							*const expiryDate = new Date(new Date(user.created_at).getTime() + user.expiry_days*  86400000);  
  
							if (nowTime &amp;gt; expiryDate.getTime()) isExpired = true;  
  
						}  
  
						if (!isExpired &amp;amp;&amp;amp; clientIP &amp;amp;&amp;amp; clientIP !== "unknown") {  
  
							let activeIps = {};  
  
							try { activeIps = JSON.parse(user.active_ips || "{}"); } catch (e) {}  
  
							let hasChanges = false;  
  
							for (const [ip, data] of Object.entries(activeIps)) {  
  
								const lastSeen = data &amp;amp;&amp;amp; typeof data === "object" ? data.timestamp : data;  
  
								if (nowTime - lastSeen &amp;gt; 20000) { delete activeIps[ip]; hasChanges = true; }  
  
							}  
  
							if (!activeIps[clientIP]) {  
  
								isIpLimitExpired = true;  
  
							} else {  
  
								const sortedIps = Object.keys(activeIps).sort((a, b) =&amp;gt; {  
  
									const tA = typeof activeIps[a] === "object" ? activeIps[a].timestamp : activeIps[a];  
  
									const tB = typeof activeIps[b] === "object" ? activeIps[b].timestamp : activeIps[b];  
  
									return tB - tA;  
  
								});  
  
								if (user.ip_limit &amp;amp;&amp;amp; user.ip_limit &amp;gt; 0 &amp;amp;&amp;amp; sortedIps.indexOf(clientIP) &amp;gt;= user.ip_limit) isIpLimitExpired = true;  
  
							}  
  
							if (hasChanges || isIpLimitExpired) updatedActiveIps = JSON.stringify(activeIps);  
  
						}  
  
					}  
  
					if (isExpired) {  
  
						await env.DB.prepare("UPDATE users SET is_active = 0, last_active = 0 WHERE uuid = ?").bind(validUUID).run();  
  
						clearTimeout(heartbeat);  
  
						closeSocketQuietly(serverSock);  
  
						return;  
  
					}  
  
					if (isIpLimitExpired) {  
  
						clearTimeout(heartbeat);  
  
						closeSocketQuietly(serverSock);  
  
						return;  
  
					}  
  
					if (updatedActiveIps !== null) {  
  
						await env.DB.prepare("UPDATE users SET last_active = ?, active_ips = ? WHERE username = ?").bind(nowTime, updatedActiveIps, username).run();  
  
					} else {  
  
						await env.DB.prepare("UPDATE users SET last_active = ? WHERE username = ?").bind(nowTime, username).run();  
  
					}  
  
				}  
  
			} catch (e) {}  
  
			heartbeat = setTimeout(runHeartbeat, Math.floor(Math.random()  *5000) + 20000);*  
  
		*} else {*  
  
			*clearTimeout(heartbeat);*  
  
		*}*  
  
	*};*  
  
	*heartbeat = setTimeout(runHeartbeat, Math.floor(Math.random()*  5000) + 20000);  
  
	let remoteConnWrapper = { socket: null, connectingPromise: null, retryConnect: null };  
  
	let reqUUID = null;  
  
	let isHeaderParsed = false;  
  
	let isHeaderParsing = false;  
  
	let isDnsQuery = false;  
  
	let chunkBuffer = new Uint8Array(0);  
  
	let uncountedBytes = 0;  
  
	const proxyIP = storedData?.proxy_ip || "";  
  
	let wsChain = Promise.resolve();  
  
	let wsStopped = false,  
  
		wsFailed = false,  
  
		wsFinished = false;  
  
	let wsQueueBytes = 0,  
  
		wsQueueItems = 0;  
  
	let currentSocketWriter = null,  
  
		activeRemoteWriter = null;  
  
	const releaseRemoteWriter = () =&amp;gt; {  
  
		if (activeRemoteWriter) {  
  
			try {  
  
				activeRemoteWriter.releaseLock();  
  
			} catch (e) {}  
  
			activeRemoteWriter = null;  
  
		}  
  
		currentSocketWriter = null;  
  
	};  
  
	const getRemoteWriter = () =&amp;gt; {  
  
		const s = remoteConnWrapper.socket;  
  
		if (!s) return null;  
  
		if (s !== currentSocketWriter) {  
  
			releaseRemoteWriter();  
  
			currentSocketWriter = s;  
  
			activeRemoteWriter = s.writable.getWriter();  
  
		}  
  
		return activeRemoteWriter;  
  
	};  
  
	const upstreamQueue = createUpstreamQueue({  
  
		getWriter: getRemoteWriter,  
  
		releaseWriter: releaseRemoteWriter,  
  
		retryConnect: async () =&amp;gt; {  
  
			if (typeof remoteConnWrapper.retryConnect === "function") {  
  
				await remoteConnWrapper.retryConnect();  
  
			}  
  
		},  
  
		closeConnection: () =&amp;gt; {  
  
			try {  
  
				remoteConnWrapper.socket?.close();  
  
			} catch (e) {}  
  
			closeSocketQuietly(serverSock);  
  
		},  
  
		name: "vIeesWSQueue",  
  
	});  
  
	const writeToRemote = async (chunk, allowRetry = true) =&amp;gt; {  
  
		return upstreamQueue.writeAndAwait(chunk, allowRetry);  
  
	};  
  
	const processWsMessage = async (chunk) =&amp;gt; {  
  
		const bytes = chunk.byteLength || 0;  
  
		await addBytes(bytes);  
  
		if (isDnsQuery) {  
  
			await forwardVlessUDP(chunk, serverSock, null, addBytes, targetDns);  
  
			return;  
  
		}  
  
		if (isHeaderParsed) {  
  
			if (remoteConnWrapper.connectingPromise) {  
  
				await remoteConnWrapper.connectingPromise;  
  
			}  
  
			await writeToRemote(chunk);  
  
			return;  
  
		}  
  
		if (!isHeaderParsed) {  
  
			chunkBuffer = concatBytes(chunkBuffer, chunk);  
  
			if (chunkBuffer.byteLength &amp;lt; 24) return;  
  
			  
  
			let optLen = chunkBuffer[17];  
  
			let requiredLen = 18 + optLen + 4;   
  
			if (chunkBuffer.byteLength &amp;lt; requiredLen) return;  
  
			  
  
			let addrType = chunkBuffer[18 + optLen + 3];  
  
			if (addrType === 1) {  
  
				requiredLen += 4;  
  
			} else if (addrType === 2) {  
  
				requiredLen += 1;  
  
				if (chunkBuffer.byteLength &amp;lt; requiredLen) return;  
  
				requiredLen += chunkBuffer[18 + optLen + 4];  
  
			} else if (addrType === 3) {  
  
				requiredLen += 16;  
  
			}  
  
			  
  
			if (chunkBuffer.byteLength &amp;lt; requiredLen) return;  
  
			if (isHeaderParsing) return;  
  
			isHeaderParsing = true;  
  
			reqUUID = extractUUIDFromvIees(chunkBuffer);  
  
			if (!reqUUID) {  
  
				serverSock.close();  
  
				return;  
  
			}  
  
			let user = null;  
  
			try {  
  
				user = await env.DB.prepare("SELECT  *FROM users WHERE uuid = ?").bind(reqUUID).first();*  
  
			*} catch (e) {}*  
  
			*if (!user) {*  
  
				*serverSock.close();*  
  
				*return;*  
  
			*}*  
  
			*username = user.username;*  
  
			*validUUID = reqUUID;*  
  
			*let currentReqs = USER_REQ_CACHE.get(username) || 0;*  
  
			*USER_REQ_CACHE.set(username, currentReqs + 1);*  
  
			*if (!GLOBAL_TRAFFIC_CACHE.has(username)) {*  
  
				*GLOBAL_TRAFFIC_CACHE.set(username, 0);*  
  
			*}*  
  
			*if (isOfflineSet || serverSock.readyState !== [[WebSocket.OPEN](http://WebSocket.OPEN)]([http://WebSocket.OPEN](http://WebSocket.OPEN))) {*  
  
				*return;*  
  
			*}*  
  
			*if ([[user.is](http://user.is)]([http://user.is)_active](http://user.is)_active) === 0) {*  
  
				*serverSock.close();*  
  
				*return;*  
  
			*}*  
  
			*if (user.limit_gb &amp;amp;&amp;amp; user.used_gb &amp;gt;= user.limit_gb) {*  
  
				*serverSock.close();*  
  
				*return;*  
  
			*}*  
  
			*if (user.limit_req &amp;amp;&amp;amp; user.used_req + (USER_REQ_CACHE.get(username) || 0) &amp;gt; user.limit_req) {*  
  
				*serverSock.close();*  
  
				*return;*  
  
			*}*  
  
			*if (user.expiry_days &amp;amp;&amp;amp; user.created_at) {*  
  
				*const created = new Date(user.created_at);*  
  
				*const expiryDate = new Date(created.getTime() + user.expiry_days*  24*  60  *60*  1000);  
  
				if (new Date() &amp;gt; expiryDate) {  
  
					try {  
  
						await env.DB.prepare("UPDATE users SET is_active = 0, last_active = 0 WHERE uuid = ?").bind(reqUUID).run();  
  
					} catch (e) {}  
  
					serverSock.close();  
  
					return;  
  
				}  
  
			}  
  
			if (user.block_porn === 1 &amp;amp;&amp;amp; user.block_ads === 1) {  
  
				targetDns = "94.140.14.15";  
  
				targetDoh = "[[https://family.adguard-dns.com/dns-query](https://family.adguard-dns.com/dns-query)](https://family.adguard-dns.com/dns-query](https://family.adguard-dns.com/dns-query))";  
  
			} else if (user.block_porn === 1) {  
  
				targetDns = "1.1.1.3";  
  
				targetDoh = "[[https://family.cloudflare-dns.com/dns-query](https://family.cloudflare-dns.com/dns-query)](https://family.cloudflare-dns.com/dns-query](https://family.cloudflare-dns.com/dns-query))";  
  
			} else if (user.block_ads === 1) {  
  
				targetDns = "94.140.14.14";  
  
				targetDoh = "[[https://dns.adguard-dns.com/dns-query](https://dns.adguard-dns.com/dns-query)](https://dns.adguard-dns.com/dns-query](https://dns.adguard-dns.com/dns-query))";  
  
			}  
  
			if (clientIP &amp;amp;&amp;amp; clientIP !== "unknown") {  
  
				let activeIps = {};  
  
				try { activeIps = JSON.parse(user.active_ips || "{}"); } catch (e) {}  
  
				const now = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());  
  
				for (const [ip, data] of Object.entries(activeIps)) {  
  
					const lastSeen = data &amp;amp;&amp;amp; typeof data === "object" ? data.timestamp : data;  
  
					if (now - lastSeen &amp;gt; 20000) delete activeIps[ip];  
  
				}  
  
				let isNewIp = false;  
  
				if (!activeIps[clientIP]) {  
  
					const sortedIps = Object.keys(activeIps);  
  
					if (user.ip_limit &amp;amp;&amp;amp; user.ip_limit &amp;gt; 0 &amp;amp;&amp;amp; sortedIps.length &amp;gt;= user.ip_limit) {  
  
						serverSock.close();  
  
						return;  
  
					}  
  
					activeIps[clientIP] = { timestamp: now, count: 1 };  
  
					isNewIp = true;  
  
				} else {  
  
					if (typeof activeIps[clientIP] === "object") {  
  
						activeIps[clientIP].timestamp = now;  
  
						activeIps[clientIP].count = (activeIps[clientIP].count || 0) + 1;  
  
					} else {  
  
						activeIps[clientIP] = { timestamp: now, count: 1 };  
  
					}  
  
				}  
  
				const lastWrite = GLOBAL_LAST_ACTIVE_WRITE.get(username) || 0;  
  
				if (isNewIp || (now - lastWrite &amp;gt; 30000)) {  
  
					GLOBAL_LAST_ACTIVE_WRITE.set(username, now);  
  
					const updateTask = async () =&amp;gt; {  
  
						try {  
  
							await env.DB.prepare("UPDATE users SET active_ips = ?, last_active = ? WHERE uuid = ?").bind(JSON.stringify(activeIps), now, reqUUID).run();  
  
						} catch (e) {}  
  
					};  
  
					if (ctx) ctx.waitUntil(updateTask());  
  
					else updateTask();  
  
				}  
  
			}  
  
			isHeaderParsed = true;  
  
			let activeCount = ACTIVE_CONNECTIONS_COUNT.get(username) || 0;  
  
			ACTIVE_CONNECTIONS_COUNT.set(username, activeCount + 1);  
  
			hasCountedAsActive = true;  
  
			if (activeCount === 0) {  
  
				const setOnlineTask = async () =&amp;gt; {  
  
					try {  
  
						const now = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());  
  
						GLOBAL_LAST_ACTIVE_WRITE.set(username, now);  
  
						await env.DB.prepare("UPDATE users SET last_active = ? WHERE username = ?").bind(now, username).run();  
  
					} catch (e) {}  
  
				};  
  
				if (ctx) ctx.waitUntil(setOnlineTask());  
  
				else setOnlineTask();  
  
			}  
  
			try {  
  
				let offset = 17;  
  
				const optLen = chunkBuffer[offset++];  
  
				offset += optLen;  
  
				const cmd = chunkBuffer[offset++];  
  
				const port = (chunkBuffer[offset++] &amp;lt;&amp;lt; 8) | chunkBuffer[offset++];  
  
				const addrType = chunkBuffer[offset++];  
  
				let addr = "";  
  
				if (addrType === 1) {  
  
					addr = `${chunkBuffer[offset++]}.${chunkBuffer[offset++]}.${chunkBuffer[offset++]}.${chunkBuffer[offset++]}`;  
  
				} else if (addrType === 2) {  
  
					const domainLen = chunkBuffer[offset++];  
  
					addr = new TextDecoder().decode(chunkBuffer.slice(offset, offset + domainLen));  
  
					offset += domainLen;  
  
				} else if (addrType === 3) {  
  
					const v6 = [];  
  
					for (let i = 0; i &amp;lt; 8; i++) {  
  
						v6.push(((chunkBuffer[offset++] &amp;lt;&amp;lt; 8) | chunkBuffer[offset++]).toString(16));  
  
					}  
  
					addr = v6.join(":");  
  
				}  
  
				const rawData = chunkBuffer.slice(offset);  
  
				const respHeader = new Uint8Array([chunkBuffer[0], 0]);  
  
				if ((user.block_ads === 1 || user.block_porn === 1) &amp;amp;&amp;amp; addrType === 2 &amp;amp;&amp;amp; port !== 53) {  
  
					try {  
  
						const dnsCheck = await dohQuery(addr, "A", targetDoh);  
  
						const isBlocked = dnsCheck.some((r) =&amp;gt; [[r.data](http://r.data)]([http://r.data](http://r.data)) === "0.0.0.0" || [[r.data](http://r.data)]([http://r.data](http://r.data)) === "::" || [[r.data](http://r.data)]([http://r.data](http://r.data)) === "176.103.130.130");  
  
						if (isBlocked) {  
  
							serverSock.close();  
  
							return;  
  
						}  
  
						const resolvedRecord = dnsCheck.find((r) =&amp;gt; r.type === 1 || r.type === 28);  
  
						if (resolvedRecord &amp;amp;&amp;amp; [[resolvedRecord.data](http://resolvedRecord.data)]([http://resolvedRecord.data](http://resolvedRecord.data))) {  
  
							addr = [[resolvedRecord.data](http://resolvedRecord.data)]([http://resolvedRecord.data](http://resolvedRecord.data));  
  
						}  
  
					} catch (e) {}  
  
				}  
  
				if (cmd === 2) {  
  
					if (port === 53) {  
  
						isDnsQuery = true;  
  
						await forwardVlessUDP(rawData, serverSock, respHeader, addBytes, targetDns);  
  
					} else {  
  
						serverSock.close();  
  
					}  
  
					return;  
  
				}  
  
				if (port === 25 || port === 22 || /^(127\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|169\.254\.|::1|fd[0-9a-f]{2}:|fe80:)/i.test(addr)) {  
  
					serverSock.close();  
  
					return;  
  
				}  
  
				const connectTCP = async (dataPayload = null, useFallback = true) =&amp;gt; {  
  
					if (remoteConnWrapper.connectingPromise) {  
  
						await remoteConnWrapper.connectingPromise;  
  
						return;  
  
					}  
  
					const task = (async () =&amp;gt; {  
  
							let s = null;  
  
							const perIpProxy = storedData?.per_ip_proxy || "";  
  
							const userSocks5 = perIpProxy || user?.user_socks5 || "";  
  
							if (userSocks5) {  
  
								try {  
  
									s = await connectProxy(userSocks5, addr, port, dataPayload);  
  
								} catch (proxyErr) {  
  
									if (!perIpProxy &amp;amp;&amp;amp; [[user.auto](http://user.auto)]([http://user.auto)_rotate_user_proxy](http://user.auto)_rotate_user_proxy) === 1) {  
  
										const replaceTask = replaceBrokenProxy(user.username, env, userSocks5);  
  
										if (ctx) ctx.waitUntil(replaceTask);  
  
										else replaceTask.catch(() =&amp;gt; {});  
  
									}  
  
									if (useFallback &amp;amp;&amp;amp; proxyIP &amp;amp;&amp;amp; !perIpProxy) {  
  
										s = await connectDirect(proxyIP, port, dataPayload);  
  
									} else {  
  
										throw proxyErr;  
  
									}  
  
								}  
  
							} else {  
  
								try {  
  
									s = await connectDirect(addr, port, dataPayload);  
  
								} catch (err) {  
  
									if (useFallback &amp;amp;&amp;amp; proxyIP) {  
  
										s = await connectDirect(proxyIP, port, dataPayload);  
  
									} else {  
  
										throw err;  
  
									}  
  
								}  
  
							}  
  
						remoteConnWrapper.socket = s;  
  
						s.closed.catch(() =&amp;gt; {}).finally(() =&amp;gt; closeSocketQuietly(serverSock));  
  
						connectStreams(s, serverSock, respHeader, null, (b) =&amp;gt; {  
  
							addBytes(b);  
  
						});  
  
					})();  
  
					remoteConnWrapper.connectingPromise = task;  
  
					try {  
  
						await task;  
  
					} finally {  
  
						if (remoteConnWrapper.connectingPromise === task) {  
  
							remoteConnWrapper.connectingPromise = null;  
  
						}  
  
					}  
  
				};  
  
				remoteConnWrapper.retryConnect = async () =&amp;gt; connectTCP(null, false);  
  
				await connectTCP(rawData, true);  
  
			} catch (e) {  
  
				serverSock.close();  
  
			}  
  
		}  
  
	};  
  
	const handleWsError = (err) =&amp;gt; {  
  
		if (wsFailed) return;  
  
		wsFailed = true;  
  
		wsStopped = true;  
  
		wsQueueBytes = 0;  
  
		wsQueueItems = 0;  
  
		upstreamQueue.clear();  
  
		releaseRemoteWriter();  
  
		closeSocketQuietly(serverSock);  
  
		setOffline();  
  
	};  
  
	const pushToChain = (task) =&amp;gt; {  
  
		wsChain = wsChain.then(task).catch(handleWsError);  
  
	};  
  
	serverSock.addEventListener("message", (event) =&amp;gt; {  
  
		if (wsStopped || wsFailed) return;  
  
		const size = [[event.data](http://event.data)]([http://event.data).byteLength](http://event.data).byteLength) || 0;  
  
		const nextBytes = wsQueueBytes + size;  
  
		const nextItems = wsQueueItems + 1;  
  
		if (nextBytes &amp;gt; UPSTREAM_QUEUE_MAX_BYTES || nextItems &amp;gt; UPSTREAM_QUEUE_MAX_ITEMS) {  
  
			handleWsError(new Error("ws queue overflow"));  
  
			return;  
  
		}  
  
		wsQueueBytes = nextBytes;  
  
		wsQueueItems = nextItems;  
  
		pushToChain(async () =&amp;gt; {  
  
			wsQueueBytes = Math.max(0, wsQueueBytes - size);  
  
			wsQueueItems = Math.max(0, wsQueueItems - 1);  
  
			if (wsFailed) return;  
  
			await processWsMessage([[event.data](http://event.data)]([http://event.data](http://event.data)));  
  
		});  
  
	});  
  
	serverSock.addEventListener("close", () =&amp;gt; {  
  
		clearTimeout(heartbeat);  
  
		closeSocketQuietly(serverSock);  
  
		setOffline();  
  
		if (wsFinished) return;  
  
		wsFinished = true;  
  
		wsStopped = true;  
  
		pushToChain(async () =&amp;gt; {  
  
			if (wsFailed) return;  
  
			await upstreamQueue.awaitEmpty();  
  
			releaseRemoteWriter();  
  
		});  
  
	});  
  
	serverSock.addEventListener("error", (err) =&amp;gt; {  
  
		handleWsError(err);  
  
	});  
  
	return new Response(null, { status: 101, webSocket: clientSock });  
  
}  
  
async function getCfUsage(env) {  
  
	if (![[env.CF](http://env.CF)]([http://env.CF)_API_TOKEN](http://env.CF)_API_TOKEN) || ![[env.CF](http://env.CF)]([http://env.CF)_ACCOUNT_ID](http://env.CF)_ACCOUNT_ID)) return { today: 0, total: 0 };  
  
	try {  
  
		const now = new Date();  
  
		const startOfDay = new Date(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()).toISOString();  
  
		const thirtyDaysAgo = new Date(now.getTime() - 30  *24*  60  *60*  1000).toISOString();  
  
		const q = `query {`  
  
      `viewer {`  
  
        `accounts(filter: {accountTag: "${[env.CF](http://env.CF)_ACCOUNT_ID}"}) {`  
  
          `today: workersInvocationsAdaptive(limit: 10, filter: {datetime_geq: "${startOfDay}"}) {`  
  
            `sum { requests }`  
  
          `}`  
  
          `total: workersInvocationsAdaptive(limit: 10, filter: {datetime_geq: "${thirtyDaysAgo}"}) {`  
  
            `sum { requests }`  
  
          `}`  
  
        `}`  
  
      `}`  
  
    `}`;  
  
		const res = await fetch("[[https://api.cloudflare.com/client/v4/graphql](https://api.cloudflare.com/client/v4/graphql)](https://api.cloudflare.com/client/v4/graphql](https://api.cloudflare.com/client/v4/graphql))", {  
  
			method: "POST",  
  
			headers: { Authorization: "Bearer " + [[env.CF](http://env.CF)]([http://env.CF)_API_TOKEN](http://env.CF)_API_TOKEN), "Content-Type": "application/json" },  
  
			body: JSON.stringify({ query: q }),  
  
		});  
  
		const j = await res.json();  
  
		const acc = j?.data?.viewer?.accounts?.[0];  
  
		const todayReqs = acc?.today?.[0]?.sum?.requests || 0;  
  
		const totalReqs = acc?.total?.[0]?.sum?.requests || todayReqs;  
  
		return { today: todayReqs, total: totalReqs };  
  
	} catch (e) {  
  
		return { today: 0, total: 0 };  
  
	}  
  
}  
  
function isIPv4(value) {  
  
	const parts = String(value || "").split(".");  
  
	return parts.length === 4 &amp;amp;&amp;amp; parts.every((part) =&amp;gt; /^\d{1,3}$/.test(part) &amp;amp;&amp;amp; Number(part) &amp;gt;= 0 &amp;amp;&amp;amp; Number(part) &amp;lt;= 255);  
  
}  
  
function stripIPv6Brackets(hostname = "") {  
  
	const host = String(hostname || "").trim();  
  
	return host.startsWith("[") &amp;amp;&amp;amp; host.endsWith("]") ? host.slice(1, -1) : host;  
  
}  
  
function isIPHostname(hostname = "") {  
  
	const host = stripIPv6Brackets(hostname);  
  
	if (isIPv4(host)) return true;  
  
	if (!host.includes(":")) return false;  
  
	try {  
  
		new UR`http://[${host}]/`);  
  
		return true;  
  
	} catch (e) {  
  
		return false;  
  
	}  
  
}  
  
function convertToUint8Array(data) {  
  
	if (data instanceof Uint8Array) return data;  
  
	if (data instanceof ArrayBuffer) return new Uint8Array(data);  
  
	if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);  
  
	return new Uint8Array(data || 0);  
  
}  
  
function concatBytes(...chunkList) {  
  
	const chunks = [[chunkList.map](http://chunkList.map)]([http://chunkList.map)(convertToUint8Array)](http://chunkList.map)(convertToUint8Array));  
  
	const total = chunks.reduce((sum, c) =&amp;gt; sum + c.byteLength, 0);  
  
	const result = new Uint8Array(total);  
  
	let offset = 0;  
  
	for (const c of chunks) {  
  
		result.set(c, offset);  
  
		offset += c.byteLength;  
  
	}  
  
	return result;  
  
}  
  
function closeSocketQuietly(socket) {  
  
	try {  
  
		if (socket.readyState === [[WebSocket.OPEN](http://WebSocket.OPEN)]([http://WebSocket.OPEN](http://WebSocket.OPEN)) || socket.readyState === WebSocket.CLOSING) {  
  
			socket.close();  
  
		}  
  
	} catch (e) {}  
  
}  
  
async function dohQuery(domain, recordType, targetDoh = DOH_RESOLVER) {  
  
	const cacheKey = `${domain}:${recordType}:${targetDoh}`;  
  
	if (DNS_CACHE.has(cacheKey)) {  
  
		const cached = DNS_CACHE.get(cacheKey);  
  
		if ([[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)()) &amp;lt; cached.expires) return [[cached.data](http://cached.data)]([http://cached.data](http://cached.data));  
  
		DNS_CACHE.delete(cacheKey);  
  
	}  
  
	try {  
  
		const typeMap = { A: 1, AAAA: 28 };  
  
		const qtype = typeMap[recordType.toUpperCase()] || 1;  
  
		const encodeDomain = (name) =&amp;gt; {  
  
			const parts = name.endsWith(".") ? name.slice(0, -1).split(".") : name.split(".");  
  
			const bufs = [];  
  
			for (const label of parts) {  
  
				const enc = new TextEncoder().encode(label);  
  
				bufs.push(new Uint8Array([enc.length]), enc);  
  
			}  
  
			bufs.push(new Uint8Array([0]));  
  
			return concatBytes(...bufs);  
  
		};  
  
		const qname = encodeDomain(domain);  
  
		const query = new Uint8Array(12 + qname.length + 4);  
  
		const qview = new DataView(query.buffer);  
  
		qview.setUint16(0, crypto.getRandomValues(new Uint16Array(1))[0]);  
  
		qview.setUint16(2, 0x0100);  
  
		qview.setUint16(4, 1);  
  
		query.set(qname, 12);  
  
		qview.setUint16(12 + qname.length, qtype);  
  
		qview.setUint16(12 + qname.length + 2, 1);  
  
		const response = await fetch(targetDoh, {  
  
			method: "POST",  
  
			headers: {  
  
				"Content-Type": "application/dns-message",  
  
				Accept: "application/dns-message",  
  
			},  
  
			body: query,  
  
		});  
  
		if (!response.ok) return [];  
  
		const buf = new Uint8Array(await response.arrayBuffer());  
  
		const dv = new DataView(buf.buffer);  
  
		const qdcount = dv.getUint16(4);  
  
		const ancount = dv.getUint16(6);  
  
		const parseName = (pos) =&amp;gt; {  
  
			const labels = [];  
  
			let p = pos,  
  
				jumped = false,  
  
				endPos = -1,  
  
				safe = 128;  
  
			while (p &amp;lt; buf.length &amp;amp;&amp;amp; safe-- &amp;gt; 0) {  
  
				const len = buf[p];  
  
				if (len === 0) {  
  
					if (!jumped) endPos = p + 1;  
  
					break;  
  
				}  
  
				if ((len &amp;amp; 0xc0) === 0xc0) {  
  
					if (!jumped) endPos = p + 2;  
  
					p = ((len &amp;amp; 0x3f) &amp;lt;&amp;lt; 8) | buf[p + 1];  
  
					jumped = true;  
  
					continue;  
  
				}  
  
				labels.push(new TextDecoder().decode(buf.slice(p + 1, p + 1 + len)));  
  
				p += len + 1;  
  
			}  
  
			if (endPos === -1) endPos = p + 1;  
  
			return [labels.join("."), endPos];  
  
		};  
  
		let offset = 12;  
  
		for (let i = 0; i &amp;lt; qdcount; i++) {  
  
			const [, end] = parseName(offset);  
  
			offset = Number(end) + 4;  
  
		}  
  
		const answers = [];  
  
		for (let i = 0; i &amp;lt; ancount &amp;amp;&amp;amp; offset &amp;lt; buf.length; i++) {  
  
			const [name, nameEnd] = parseName(offset);  
  
			offset = Number(nameEnd);  
  
			const type = dv.getUint16(offset);  
  
			offset += 2;  
  
			offset += 2;  
  
			const ttl = dv.getUint32(offset);  
  
			offset += 4;  
  
			const rdlen = dv.getUint16(offset);  
  
			offset += 2;  
  
			const rdata = buf.slice(offset, offset + rdlen);  
  
			offset += rdlen;  
  
			let data;  
  
			if (type === 1 &amp;amp;&amp;amp; rdlen === 4) {  
  
				data = `${rdata[0]}.${rdata[1]}.${rdata[2]}.${rdata[3]}`;  
  
			} else if (type === 28 &amp;amp;&amp;amp; rdlen === 16) {  
  
				const segs = [];  
  
				for (let j = 0; j &amp;lt; 16; j += 2) segs.push(((rdata[j] &amp;lt;&amp;lt; 8) | rdata[j + 1]).toString(16));  
  
				data = segs.join(":");  
  
			} else {  
  
				data = Array.from(rdata)  
  
					.map((b) =&amp;gt; b.toString(16).padStart(2, "0"))  
  
					.join("");  
  
			}  
  
			answers.push({ name, type, TTL: ttl, data });  
  
		}  
  
		DNS_CACHE.set(cacheKey, { data: answers, expires: [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)()) + DNS_CACHE_TTL });  
  
		return answers;  
  
	} catch (e) {  
  
		return [];  
  
	}  
  
}  
  
function createUpstreamQueue({ getWriter, releaseWriter, retryConnect, closeConnection, name = "UpstreamQueue" }) {  
  
	let chunks = [];  
  
	let head = 0;  
  
	let queuedBytes = 0;  
  
	let draining = false;  
  
	let closed = false;  
  
	let bundleBuffer = null;  
  
	let idleResolvers = [];  
  
	let activeCompletions = null;  
  
	const settleCompletions = (completions, err = null) =&amp;gt; {  
  
		if (!completions) return;  
  
		for (const comp of completions) {  
  
			if (comp) {  
  
				if (err) comp.reject(err);  
  
				else comp.resolve();  
  
			}  
  
		}  
  
	};  
  
	const rejectQueued = (err) =&amp;gt; {  
  
		for (let i = head; i &amp;lt; chunks.length; i++) {  
  
			const item = chunks[i];  
  
			if (item &amp;amp;&amp;amp; item.completions) settleCompletions(item.completions, err);  
  
		}  
  
	};  
  
	const compact = () =&amp;gt; {  
  
		if (head &amp;gt; 32 &amp;amp;&amp;amp; head  *2 &amp;gt;= chunks.length) {*  
  
			*chunks = chunks.slice(head);*  
  
			*head = 0;*  
  
		*}*  
  
	*};*  
  
	*const resolveIdle = () =&amp;gt; {*  
  
		*if (queuedBytes || draining || !idleResolvers.length) return;*  
  
		*const resolvers = idleResolvers;*  
  
		*idleResolvers = [];*  
  
		*for (const resolve of resolvers) resolve();*  
  
	*};*  
  
	*const clear = (err = null) =&amp;gt; {*  
  
		*const closeErr = err || (closed ? new Erro*`${name}: queue closed`*) : null);*  
  
		*if (closeErr) {*  
  
			*rejectQueued(closeErr);*  
  
			*settleCompletions(activeCompletions, closeErr);*  
  
			*activeCompletions = null;*  
  
		*}*  
  
		*chunks = [];*  
  
		*head = 0;*  
  
		*queuedBytes = 0;*  
  
		*resolveIdle();*  
  
	*};*  
  
	*const shift = () =&amp;gt; {*  
  
		*if (head &amp;gt;= chunks.length) return null;*  
  
		*const item = chunks[head];*  
  
		*chunks[head++] = undefined;*  
  
		*queuedBytes -= item.chunk.byteLength;*  
  
		*compact();*  
  
		*return item;*  
  
	*};*  
  
	*const bundle = () =&amp;gt; {*  
  
		*const first = shift();*  
  
		*if (!first) return null;*  
  
		*if (head &amp;gt;= chunks.length || first.chunk.byteLength &amp;gt;= UPSTREAM_BUNDLE_TARGET_BYTES) return first;*  
  
		*let byteLength = first.chunk.byteLength;*  
  
		*let end = head;*  
  
		*let allowRetry = first.allowRetry;*  
  
		*let completions = first.completions || null;*  
  
		*while (end &amp;lt; chunks.length) {*  
  
			*const next = chunks[end];*  
  
			*const nextLength = byteLength + next.chunk.byteLength;*  
  
			*if (nextLength &amp;gt; UPSTREAM_BUNDLE_TARGET_BYTES) break;*  
  
			*byteLength = nextLength;*  
  
			*allowRetry = allowRetry &amp;amp;&amp;amp; next.allowRetry;*  
  
			*if (next.completions) completions = completions ? completions.concat(next.completions) : next.completions;*  
  
			*end++;*  
  
		*}*  
  
		*if (end === head) return first;*  
  
		*const output = (bundleBuffer ||= new Uint8Array(UPSTREAM_BUNDLE_TARGET_BYTES));*  
  
		*output.set(first.chunk);*  
  
		*let offset = first.chunk.byteLength;*  
  
		*while (head &amp;lt; end) {*  
  
			*const next = chunks[head];*  
  
			*chunks[head++] = undefined;*  
  
			*queuedBytes -= next.chunk.byteLength;*  
  
			*output.set(next.chunk, offset);*  
  
			*offset += next.chunk.byteLength;*  
  
		*}*  
  
		*compact();*  
  
		*return { chunk: output.subarray(0, byteLength), allowRetry, completions };*  
  
	*};*  
  
	*const drain = async () =&amp;gt; {*  
  
		*if (draining || closed) return;*  
  
		*draining = true;*  
  
		*try {*  
  
			*let batchCount = 0;*  
  
			*for (;;) {*  
  
				*if (closed) break;*  
  
				*const item = bundle();*  
  
				*if (!item) break;*  
  
				*let writer = getWriter();*  
  
				*if (!writer) throw new Erro*`${name}: remote writer unavailable`*);*  
  
				*const completions = item.completions || null;*  
  
				*activeCompletions = completions;*  
  
				*try {*  
  
					*try {*  
  
						*await writer.write(item.chunk);*  
  
					*} catch (err) {*  
  
						*releaseWriter?.();*  
  
						*if (!item.allowRetry || typeof retryConnect !== "function") throw err;*  
  
						*await retryConnect();*  
  
						*writer = getWriter();*  
  
						*if (!writer) throw err;*  
  
						*await writer.write(item.chunk);*  
  
					*}*  
  
					*settleCompletions(completions);*  
  
				*} catch (err) {*  
  
					*settleCompletions(completions, err);*  
  
					*throw err;*  
  
				*} finally {*  
  
					*if (activeCompletions === completions) activeCompletions = null;*  
  
				*}*  
  
				*batchCount++;*  
  
				*if (batchCount &amp;gt;= 16) {*  
  
					*await new Promise((resolve) =&amp;gt; setTimeout(resolve, 0));*  
  
					*batchCount = 0;*  
  
				*}*  
  
			*}*  
  
		*} catch (err) {*  
  
			*closed = true;*  
  
			*clear(err);*  
  
			*try {*  
  
				*closeConnection?.(err);*  
  
			*} catch (_) {}*  
  
		*} finally {*  
  
			*draining = false;*  
  
			*if (!closed &amp;amp;&amp;amp; head &amp;lt; chunks.length) setTimeout(drain, 0);*  
  
			*else resolveIdle();*  
  
		*}*  
  
	*};*  
  
	*const enqueue = (data, allowRetry = true, waitForFlush = false) =&amp;gt; {*  
  
		*if (closed) return false;*  
  
		*if (!getWriter()) return false;*  
  
		*const chunk = convertToUint8Array(data);*  
  
		*if (!chunk.byteLength) return true;*  
  
		*const nextBytes = queuedBytes + chunk.byteLength;*  
  
		*const nextItems = chunks.length - head + 1;*  
  
		*if (nextBytes &amp;gt; UPSTREAM_QUEUE_MAX_BYTES || nextItems &amp;gt; UPSTREAM_QUEUE_MAX_ITEMS) {*  
  
			*closed = true;*  
  
			*const err = Object.assign(new Erro*`${name}: upload queue overflow (${nextBytes}B/${nextItems})`*), { isQueueOverflow: true });*  
  
			*clear(err);*  
  
			*try {*  
  
				*closeConnection?.(err);*  
  
			*} catch (_) {}*  
  
			*throw err;*  
  
		*}*  
  
		*let completionPromise = null;*  
  
		*let completions = null;*  
  
		*if (waitForFlush) {*  
  
			*completions = [];*  
  
			*completionPromise = new Promise((resolve, reject) =&amp;gt; completions.push({ resolve, reject }));*  
  
		*}*  
  
		*chunks.push({ chunk, allowRetry, completions });*  
  
		*queuedBytes = nextBytes;*  
  
		*if (!draining) setTimeout(drain, 0);*  
  
		*return waitForFlush ? completionPromise.then(() =&amp;gt; true) : true;*  
  
	*};*  
  
	*return {*  
  
		*writeAndAwait(data, allowRetry = true) {*  
  
			*return enqueue(data, allowRetry, true);*  
  
		*},*  
  
		*async awaitEmpty() {*  
  
			*if (!queuedBytes &amp;amp;&amp;amp; !draining) return;*  
  
			*await new Promise((resolve) =&amp;gt; idleResolvers.push(resolve));*  
  
		*},*  
  
		*clear() {*  
  
			*closed = true;*  
  
			*clear();*  
  
		*},*  
  
	*};*  
  
*}*  
  
*function createDownstreamSender(webSocket, headerData = null) {*  
  
	*const packetCap = DOWNSTREAM_GRAIN_BYTES;*  
  
	*const tailBytes = DOWNSTREAM_GRAIN_TAIL_THRESHOLD;*  
  
	*const lowWaterBytes = Math.max(4096, tailBytes &amp;lt;&amp;lt; 3);*  
  
	*let header = headerData;*  
  
	*let pendingBuffer = new Uint8Array(packetCap);*  
  
	*let pendingBytes = 0;*  
  
	*let flushTimer = null;*  
  
	*let taskQueued = false;*  
  
	*let generation = 0;*  
  
	*let scheduledGeneration = 0;*  
  
	*let waitRounds = 0;*  
  
	*let flushPromise = null;*  
  
	*const sendRawChunk = async (chunk) =&amp;gt; {*  
  
		*if (webSocket.readyState !== [[WebSocket.OPEN](http://WebSocket.OPEN)]([http://WebSocket.OPEN](http://WebSocket.OPEN))) throw new Error("ws.readyState is not open");*  
  
		*webSocket.send(chunk);*  
  
	*};*  
  
	*const attachResponseHeader = (chunk) =&amp;gt; {*  
  
		*if (!header) return chunk;*  
  
		*const merged = new Uint8Array(header.length + chunk.byteLength);*  
  
		*merged.set(header, 0);*  
  
		*merged.set(chunk, header.length);*  
  
		*header = null;*  
  
		*return merged;*  
  
	*};*  
  
	*const flush = async () =&amp;gt; {*  
  
		*while (flushPromise) await flushPromise;*  
  
		*if (flushTimer) clearTimeout(flushTimer);*  
  
		*flushTimer = null;*  
  
		*taskQueued = false;*  
  
		*if (!pendingBytes) return;*  
  
		*const output = pendingBuffer.subarray(0, pendingBytes).slice();*  
  
		*pendingBuffer = new Uint8Array(packetCap);*  
  
		*pendingBytes = 0;*  
  
		*waitRounds = 0;*  
  
		*flushPromise = sendRawChunk(output).finally(() =&amp;gt; {*  
  
			*flushPromise = null;*  
  
		*});*  
  
		*return flushPromise;*  
  
	*};*  
  
	*const scheduleFlush = () =&amp;gt; {*  
  
		*if (flushTimer || taskQueued) return;*  
  
		*taskQueued = true;*  
  
		*scheduledGeneration = generation;*  
  
		*setTimeout(() =&amp;gt; {*  
  
			*taskQueued = false;*  
  
			*if (!pendingBytes || flushTimer) return;*  
  
			*if (packetCap - pendingBytes &amp;lt; tailBytes) {*  
  
				*flush().catch(() =&amp;gt; closeSocketQuietly(webSocket));*  
  
				*return;*  
  
			*}*  
  
			*flushTimer = setTimeout(*  
  
				*() =&amp;gt; {*  
  
					*flushTimer = null;*  
  
					*if (!pendingBytes) return;*  
  
					*if (packetCap - pendingBytes &amp;lt; tailBytes) {*  
  
						*flush().catch(() =&amp;gt; closeSocketQuietly(webSocket));*  
  
						*return;*  
  
					*}*  
  
					*if (waitRounds &amp;lt; 2 &amp;amp;&amp;amp; (generation !== scheduledGeneration || pendingBytes &amp;lt; lowWaterBytes)) {*  
  
						*waitRounds++;*  
  
						*scheduledGeneration = generation;*  
  
						*scheduleFlush();*  
  
						*return;*  
  
					*}*  
  
					*flush().catch(() =&amp;gt; closeSocketQuietly(webSocket));*  
  
				*},*  
  
				*Math.max(DOWNSTREAM_GRAIN_SILENT_MS, 1),*  
  
			*);*  
  
		*}, 0);*  
  
	*};*  
  
	*return {*  
  
		*async sendDirect(data) {*  
  
			*let chunk = convertToUint8Array(data);*  
  
			*if (!chunk.byteLength) return;*  
  
			*chunk = attachResponseHeader(chunk);*  
  
			*await sendRawChunk(chunk);*  
  
		*},*  
  
		*async send(data) {*  
  
			*let chunk = convertToUint8Array(data);*  
  
			*if (!chunk.byteLength) return;*  
  
			*chunk = attachResponseHeader(chunk);*  
  
			*let offset = 0;*  
  
			*const totalBytes = chunk.byteLength;*  
  
			*while (offset &amp;lt; totalBytes) {*  
  
				*if (!pendingBytes &amp;amp;&amp;amp; totalBytes - offset &amp;gt;= packetCap) {*  
  
					*const sendBytes = Math.min(packetCap, totalBytes - offset);*  
  
					*const view = offset || sendBytes !== totalBytes ? chunk.subarray(offset, offset + sendBytes) : chunk;*  
  
					*await sendRawChunk(view);*  
  
					*offset += sendBytes;*  
  
					*continue;*  
  
				*}*  
  
				*const copyBytes = Math.min(packetCap - pendingBytes, totalBytes - offset);*  
  
				*pendingBuffer.set(chunk.subarray(offset, offset + copyBytes), pendingBytes);*  
  
				*pendingBytes += copyBytes;*  
  
				*offset += copyBytes;*  
  
				*generation++;*  
  
				*if (pendingBytes === packetCap || packetCap - pendingBytes &amp;lt; tailBytes) await flush();*  
  
				*else scheduleFlush();*  
  
			*}*  
  
		*},*  
  
		*flush,*  
  
	*};*  
  
*}*  
  
*async function waitForBackpressure(ws) {*  
  
	*if (typeof ws.bufferedAmount === "number") {*  
  
		*let maxAttempts = 300;*  
  
		*while (ws.bufferedAmount &amp;gt; 512*  1024 &amp;amp;&amp;amp; maxAttempts &amp;gt; 0) {  
  
			if (ws.readyState !== [[WebSocket.OPEN](http://WebSocket.OPEN)]([http://WebSocket.OPEN](http://WebSocket.OPEN))) break;  
  
			await new Promise((r) =&amp;gt; setTimeout(r, 25));  
  
			maxAttempts--;  
  
		}  
  
	}  
  
}  
  
async function connectStreams(remoteSocket, webSocket, headerData, retryFunc, onBytes) {  
  
	let header = headerData,  
  
		hasData = false,  
  
		reader,  
  
		useBYOB = false;  
  
	const BYOB_LIMIT = 64  *1024;*  
  
	*const downstreamSender = createDownstreamSender(webSocket, header);*  
  
	*header = null;*  
  
	*try {*  
  
		*reader = remoteSocket.readable.getReader({ mode: "byob" });*  
  
		*useBYOB = true;*  
  
	*} catch (e) {*  
  
		*reader = remoteSocket.readable.getReader();*  
  
	*}*  
  
	*try {*  
  
		*if (!useBYOB) {*  
  
			*while (true) {*  
  
				*await waitForBackpressure(webSocket);*  
  
				*const { done, value } = await [[reader.read](http://reader.read)]([http://reader.read)()](http://reader.read)());*  
  
				*if (done) break;*  
  
				*if (!value || value.byteLength === 0) continue;*  
  
				*hasData = true;*  
  
				*if (typeof onBytes === "function") onBytes(value.byteLength);*  
  
				*await downstreamSender.send(value);*  
  
			*}*  
  
		*} else {*  
  
			*let readBuffer = new ArrayBuffer(BYOB_LIMIT);*  
  
			*while (true) {*  
  
				*await waitForBackpressure(webSocket);*  
  
				*const { done, value } = await [[reader.read](http://reader.read)]([http://reader.read)(new](http://reader.read)(new) Uint8Array(readBuffer, 0, BYOB_LIMIT));*  
  
				*if (done) break;*  
  
				*if (!value || value.byteLength === 0) continue;*  
  
				*hasData = true;*  
  
				*if (typeof onBytes === "function") onBytes(value.byteLength);*  
  
				*if (value.byteLength &amp;gt;= DOWNSTREAM_GRAIN_BYTES) {*  
  
					*await downstreamSender.flush();*  
  
					*await downstreamSender.sendDirect(value);*  
  
					*readBuffer = new ArrayBuffer(BYOB_LIMIT);*  
  
				*} else {*  
  
					*await downstreamSender.send(value);*  
  
					*readBuffer = value.buffer.byteLength &amp;gt;= BYOB_LIMIT ? value.buffer : new ArrayBuffer(BYOB_LIMIT);*  
  
				*}*  
  
			*}*  
  
		*}*  
  
		*await downstreamSender.flush();*  
  
	*} catch (err) {*  
  
		*closeSocketQuietly(webSocket);*  
  
	*} finally {*  
  
		*try {*  
  
			*reader.cancel();*  
  
		*} catch (e) {}*  
  
		*try {*  
  
			*reader.releaseLock();*  
  
		*} catch (e) {}*  
  
	*}*  
  
	*if (!hasData &amp;amp;&amp;amp; retryFunc) await retryFunc();*  
  
*}*  
  
*async function buildRaceCandidates(address, port) {*  
  
	*if (!PRELOAD_RACE_DIAL || isIPHostname(address)) return null;*  
  
	*const [aRecords, aaaaRecords] = await Promise.all([dohQuery(address, "A"), dohQuery(address, "AAAA")]);*  
  
	*const ipv4List = [*  
  
		*...new Set(*  
  
			*aRecords.flatMap((r) =&amp;gt; {*  
  
				*return r.type === 1 &amp;amp;&amp;amp; typeof [[r.data](http://r.data)]([http://r.data](http://r.data)) === "string" &amp;amp;&amp;amp; isIPv4([[r.data](http://r.data)]([http://r.data](http://r.data))) ? [[[r.data](http://r.data)]([http://r.data](http://r.data))] : [];*  
  
			*}),*  
  
		*),*  
  
	*];*  
  
	*const ipv6List = [*  
  
		*...new Set(*  
  
			*aaaaRecords.flatMap((r) =&amp;gt; {*  
  
				*return r.type === 28 &amp;amp;&amp;amp; typeof [[r.data](http://r.data)]([http://r.data](http://r.data)) === "string" &amp;amp;&amp;amp; isIPHostname([[r.data](http://r.data)]([http://r.data](http://r.data))) ? [[[r.data](http://r.data)]([http://r.data](http://r.data))] : [];*  
  
			*}),*  
  
		*),*  
  
	*];*  
  
	*const limit = Math.max(1, TCP_CONCURRENCY || 0);*  
  
	*const ipList = ipv4List.length &amp;gt;= limit ? ipv4List.slice(0, limit) : ipv4List.concat(ipv6List.slice(0, limit - ipv4List.length));*  
  
	*if (ipList.length === 0) return null;*  
  
	*return [[ipList.map](http://ipList.map)]([http://ipList.map)((hostname](http://ipList.map)((hostname), attempt) =&amp;gt; ({ hostname, port, attempt, resolvedFrom: address }));*  
  
*}*  
  
*async function connectDirect(address, port, initialData = null) {*  
  
	*const raceCandidates = await buildRaceCandidates(address, port);*  
  
	*const candidates = raceCandidates || Array.from({ length: TCP_CONCURRENCY }, () =&amp;gt; ({ hostname: address, port }));*  
  
	*const openConnection = async (host, prt) =&amp;gt; {*  
  
		*const socket = connect({ hostname: host, port: prt });*  
  
		*await Promise.race([socket.opened, new Promise((_, reject) =&amp;gt; setTimeout(() =&amp;gt; reject(new Error("timeout")), 1000))]);*  
  
		*return socket;*  
  
	*};*  
  
	*if (candidates.length === 1) {*  
  
		*const s = await openConnection(candidates[0].hostname, candidates[0].port);*  
  
		*if (initialData &amp;amp;&amp;amp; initialData.byteLength &amp;gt; 0) {*  
  
			*const w = s.writable.getWriter();*  
  
			*await w.write(convertToUint8Array(initialData));*  
  
			*w.releaseLock();*  
  
		*}*  
  
		*return s;*  
  
	*}*  
  
	*const attempts = [[candidates.map](http://candidates.map)]([http://candidates.map)((c](http://candidates.map)((c)) =&amp;gt; openConnection(c.hostname, c.port).then((socket) =&amp;gt; ({ socket, candidate: c })));*  
  
	*let winner = null;*  
  
	*try {*  
  
		*winner = await Promise.any(attempts);*  
  
		*if (initialData &amp;amp;&amp;amp; initialData.byteLength &amp;gt; 0) {*  
  
			*const w = winner.socket.writable.getWriter();*  
  
			*await w.write(convertToUint8Array(initialData));*  
  
			*w.releaseLock();*  
  
		*}*  
  
		*return winner.socket;*  
  
	*} finally {*  
  
		*if (winner) {*  
  
			*for (const attempt of attempts) {*  
  
				*attempt*  
  
					*.then(({ socket }) =&amp;gt; {*  
  
						*if (socket !== winner.socket) {*  
  
							*try { socket.close(); } catch (e) {}*  
  
						*}*  
  
					*})*  
  
					*.catch(() =&amp;gt; {});*  
  
			*}*  
  
		*}*  
  
	*}*  
  
*}*  
  
*async function forwardVlessUDP(udpChunk, webSocket, respHeader, onBytes, dnsServer = "8.8.4.4") {*  
  
    *const requestData = convertToUint8Array(udpChunk);*  
  
    *try {*  
  
        *const tcpSocket = connect({ hostname: dnsServer, port: 53 });*  
  
        *let vIeesHeader = respHeader;*  
  
        *const writer = tcpSocket.writable.getWriter();*  
  
        **  
  
        *await writer.write(requestData);*  
  
        *writer.releaseLock();*  
  
        **  
  
        *await tcpSocket.readable.pipeTo(*  
  
            *new WritableStream({*  
  
                *async write(chunk) {*  
  
                    *const rawResponse = convertToUint8Array(chunk);*  
  
                    *if (typeof onBytes === "function") onBytes(rawResponse.byteLength);*  
  
                    *if (webSocket.readyState !== [[WebSocket.OPEN](http://WebSocket.OPEN)]([http://WebSocket.OPEN](http://WebSocket.OPEN))) return;*  
  
                    *if (vIeesHeader) {*  
  
                        *const merged = new Uint8Array(vIeesHeader.length + rawResponse.byteLength);*  
  
                        *merged.set(vIeesHeader, 0);*  
  
                        *merged.set(rawResponse, vIeesHeader.length);*  
  
                        *webSocket.send(merged.buffer);*  
  
                        *vIeesHeader = null;*  
  
                    *} else {*  
  
                        *webSocket.send(rawResponse);*  
  
                    *}*  
  
                *},*  
  
            *}),*  
  
        *);*  
  
    *} catch (e) {}*  
  
*}*  
  
*function extractUUIDFromvIees(data) {*  
  
	*if (data.byteLength &amp;lt; 17) return null;*  
  
	*let uuid = "";*  
  
	*for (let i = 1; i &amp;lt; 17; i++) {*  
  
		*uuid += LUT_HEX[data[i]];*  
  
		*if (i === 4 || i === 6 || i === 8 || i === 10) uuid += "-";*  
  
	*}*  
  
	*return uuid;*  
  
*}*  
  
*function trackRequest(env, ctx) {*  
  
	*GLOBAL_REQ_COUNT++;*  
  
	*const now = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());*  
  
	*if (now - GLOBAL_LAST_REQ_WRITE &amp;gt; 60000 &amp;amp;&amp;amp; GLOBAL_REQ_COUNT &amp;gt; 0) {*  
  
		*GLOBAL_LAST_REQ_WRITE = now;*  
  
		*const countToSave = GLOBAL_REQ_COUNT;*  
  
		*GLOBAL_REQ_COUNT = 0;*  
  
		*const task = async () =&amp;gt; {*  
  
			*try {*  
  
				*const today = new Date().toISOString().split("T")[0];*  
  
				*await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_total', ?) ON CONFLICT(key) DO UPDATE SET value = CAST((CAST(value AS INTEGER) + ?) AS TEXT)").bind(String(countToSave), countToSave).run();*  
  
				*const lastDateRow = await env.DB.prepare("SELECT value FROM settings WHERE key = 'req_last_date'").first();*  
  
				*if (!lastDateRow || lastDateRow.value !== today) {*  
  
					*await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_last_date', ?) ON CONFLICT(key) DO UPDATE SET value = ?").bind(today, today).run();*  
  
					*await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_today', ?) ON CONFLICT(key) DO UPDATE SET value = ?").bind(String(countToSave), String(countToSave)).run();*  
  
				*} else {*  
  
					*await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_today', ?) ON CONFLICT(key) DO UPDATE SET value = CAST((CAST(value AS INTEGER) + ?) AS TEXT)").bind(String(countToSave), countToSave).run();*  
  
				*}*  
  
			*} catch (e) {*  
  
				*console.error("Track DB Error:", e.message);*  
  
			*}*  
  
		*};*  
  
		*if (ctx) ctx.waitUntil(task());*  
  
		*else task();*  
  
	*}*  
  
*}*  
  
*async function connectProxy(proxyStr, destAddr, destPort, initialData) {*  
  
	*let normalized = proxyStr;*  
  
	*if (proxyStr.includes("t.me/socks") || proxyStr.includes("tg://socks")) {*  
  
		*const server = proxyStr.match(/server=([^&amp;amp;]+)/)?.[1];*  
  
		*const port = proxyStr.match(/port=([^&amp;amp;]+)/)?.[1];*  
  
		*const user = proxyStr.match(/user=([^&amp;amp;]+)/)?.[1];*  
  
		*const pass = proxyStr.match(/pass=([^&amp;amp;]+)/)?.[1];*  
  
		*if (server &amp;amp;&amp;amp; port) {*  
  
			*normalized = user &amp;amp;&amp;amp; pass ?* `socks5://${user}:${pass}@${server}:${port}` *:* `socks5://${server}:${port}`*;*  
  
		*}*  
  
	*}*  
  
	*const isHttp = normalized.toLowerCase().startsWith("http://") || normalized.toLowerCase().startsWith("https://");*  
  
	*const isSocks4 = normalized.toLowerCase().startsWith("socks4://");*  
  
	*let cleanStr = normalized.replace(/^(socks4|socks5|socks|http|https):\/\//i, "");*  
  
	*if (isHttp) {*  
  
		*return await connectHttp(cleanStr, destAddr, destPort, initialData);*  
  
	*}*  
  
	*if (isSocks4) {*  
  
		*return await connectSocks4(cleanStr, destAddr, destPort, initialData);*  
  
	*}*  
  
	*return await connectSocks5(cleanStr, destAddr, destPort, initialData);*  
  
*}*  
  
*async function connectSocks4(proxyStr, destAddr, destPort, initialData) {*  
  
	*const { user, pass, host, port, auth } = parseProxyConfig(proxyStr, 1080);*  
  
	*const socket = connect({ hostname: host, port: port });*  
  
	*const reader = socket.readable.getReader();*  
  
	*const writer = socket.writable.getWriter();*  
  
	*try {*  
  
		*const portHigh = (destPort &amp;gt;&amp;gt; 8) &amp;amp; 0xff;*  
  
		*const portLow = destPort &amp;amp; 0xff;*  
  
		*let req;*  
  
		*if (isIPv4(destAddr)) {*  
  
			*const ipBytes = destAddr.split(".").map(Number);*  
  
			*req = new Uint8Array([0x04, 0x01, portHigh, portLow, ipBytes[0], ipBytes[1], ipBytes[2], ipBytes[3], 0x00]);*  
  
		*} else {*  
  
			*const hostBytes = new TextEncoder().encode(destAddr);*  
  
			*req = new Uint8Array(9 + hostBytes.length + 1);*  
  
			*req[0] = 0x04;*  
  
			*req[1] = 0x01;*  
  
			*req[2] = portHigh;*  
  
			*req[3] = portLow;*  
  
			*req[4] = 0x00;*  
  
			*req[5] = 0x00;*  
  
			*req[6] = 0x00;*  
  
			*req[7] = 0x01;*  
  
			*req[8] = 0x00;*  
  
			*req.set(hostBytes, 9);*  
  
			*req[9 + hostBytes.length] = 0x00;*  
  
		*}*  
  
		*await writer.write(req);*  
  
		*let res = await [[reader.read](http://reader.read)]([http://reader.read)()](http://reader.read)());*  
  
		*if (res.done || !res.value || res.value[0] !== 0x00 || res.value[1] !== 0x5a) {*  
  
			*throw new Error("پـروکـسـی SOCKS4 وصل نشد یا اتصال را رد کرد");*  
  
		*}*  
  
		*if (initialData &amp;amp;&amp;amp; initialData.byteLength &amp;gt; 0) {*  
  
			*await writer.write(convertToUint8Array(initialData));*  
  
		*}*  
  
		*writer.releaseLock();*  
  
		*reader.releaseLock();*  
  
		*return socket;*  
  
	*} catch (e) {*  
  
		*try {*  
  
			*writer.releaseLock();*  
  
		*} catch (err) {}*  
  
		*try {*  
  
			*reader.releaseLock();*  
  
		*} catch (err) {}*  
  
		*try {*  
  
			*socket.close();*  
  
		*} catch (err) {}*  
  
		*throw e;*  
  
	*}*  
  
*}*  
  
*function parseProxyConfig(proxyStr, defaultPort) {*  
  
	*let user = "",*  
  
		*pass = "",*  
  
		*host = "",*  
  
		*port = defaultPort;*  
  
	*let auth = false,*  
  
		*remain = proxyStr;*  
  
	*if (remain.includes("@")) {*  
  
		*const atIdx = remain.lastIndexOf("@");*  
  
		*const authPart = remain.substring(0, atIdx);*  
  
		*remain = remain.substring(atIdx + 1);*  
  
		*const colonIdx = authPart.indexOf(":");*  
  
		*if (colonIdx !== -1) {*  
  
			*user = authPart.substring(0, colonIdx);*  
  
			*pass = authPart.substring(colonIdx + 1);*  
  
		*} else {*  
  
			*user = authPart;*  
  
		*}*  
  
		*auth = true;*  
  
	*}*  
  
	*if (remain.startsWith("[")) {*  
  
		*const closeIdx = remain.indexOf("]");*  
  
		*if (closeIdx !== -1) {*  
  
			*host = remain.substring(1, closeIdx);*  
  
			*if (remain.length &amp;gt; closeIdx + 1 &amp;amp;&amp;amp; remain[closeIdx + 1] === ":") port = parseInt(remain.substring(closeIdx + 2)) || defaultPort;*  
  
		*}*  
  
	*} else {*  
  
		*const lastColon = remain.lastIndexOf(":");*  
  
		*if (lastColon !== -1 &amp;amp;&amp;amp; remain.indexOf(":") === lastColon) {*  
  
			*host = remain.substring(0, lastColon);*  
  
			*port = parseInt(remain.substring(lastColon + 1)) || defaultPort;*  
  
		*} else {*  
  
			*host = remain;*  
  
		*}*  
  
	*}*  
  
	*return { user, pass, host, port, auth };*  
  
*}*  
  
*async function connectSocks5(socksStr, destAddr, destPort, initialData) {*  
  
	*const { user, pass, host, port, auth } = parseProxyConfig(socksStr, 1080);*  
  
	*const socket = connect({ hostname: host, port: port });*  
  
	*const reader = socket.readable.getReader();*  
  
	*const writer = socket.writable.getWriter();*  
  
	*try {*  
  
		*if (auth) {*  
  
			*await writer.write(new Uint8Array([0x05, 0x02, 0x00, 0x02]));*  
  
		*} else {*  
  
			*await writer.write(new Uint8Array([0x05, 0x01, 0x00]));*  
  
		*}*  
  
		*let res = await [[reader.read](http://reader.read)]([http://reader.read)()](http://reader.read)());*  
  
		*if (res.done || !res.value || res.value[0] !== 0x05) throw new Error("پاسخ نامعتبر از سرور (پـروکـسـی SOCKS5 نیست یا خاموش است)");*  
  
		*const method = res.value[1];*  
  
		*if (method === 0x02) {*  
  
			*const uEnc = new TextEncoder().encode(user);*  
  
			*const pEnc = new TextEncoder().encode(pass);*  
  
			*const authReq = new Uint8Array(1 + 1 + uEnc.length + 1 + pEnc.length);*  
  
			*authReq[0] = 0x01;*  
  
			*authReq[1] = uEnc.length;*  
  
			*authReq.set(uEnc, 2);*  
  
			*authReq[2 + uEnc.length] = pEnc.length;*  
  
			*authReq.set(pEnc, 3 + uEnc.length);*  
  
			*await writer.write(authReq);*  
  
			*let authRes = await [[reader.read](http://reader.read)]([http://reader.read)()](http://reader.read)());*  
  
			*if (authRes.done || !authRes.value || authRes.value[1] !== 0x00) throw new Error("نام کاربری یا رمز عبور پـروکـسـی اشتباه است");*  
  
		*}*  
  
		*let addrType = 0x03;*  
  
		*let addrBytes;*  
  
		*if (isIPv4(destAddr)) {*  
  
			*addrType = 0x01;*  
  
			*addrBytes = new Uint8Array(destAddr.split(".").map(Number));*  
  
		*} else if (destAddr.includes(":")) {*  
  
			*addrType = 0x04;*  
  
			*addrBytes = new Uint8Array(16);*  
  
			*const blocks = destAddr.split(":");*  
  
			*for (let i = 0; i &amp;lt; 8; i++) {*  
  
				*const val = parseInt(blocks[i] || "0", 16);*  
  
				*addrBytes[i*  2] = (val &amp;gt;&amp;gt; 8) &amp;amp; 0xff;  
  
				addrBytes[i  *2 + 1] = val &amp;amp; 0xff;*  
  
			*}*  
  
		*} else {*  
  
			*const enc = new TextEncoder().encode(destAddr);*  
  
			*addrBytes = new Uint8Array(1 + enc.length);*  
  
			*addrBytes[0] = enc.length;*  
  
			*addrBytes.set(enc, 1);*  
  
		*}*  
  
		*const req = new Uint8Array(4 + addrBytes.length + 2);*  
  
		*req[0] = 0x05;*  
  
		*req[1] = 0x01;*  
  
		*req[2] = 0x00;*  
  
		*req[3] = addrType;*  
  
		*req.set(addrBytes, 4);*  
  
		*const portOffset = 4 + addrBytes.length;*  
  
		*req[portOffset] = (destPort &amp;gt;&amp;gt; 8) &amp;amp; 0xff;*  
  
		*req[portOffset + 1] = destPort &amp;amp; 0xff;*  
  
		*await writer.write(req);*  
  
		*let connRes = await [[reader.read](http://reader.read)]([http://reader.read)()](http://reader.read)());*  
  
		*if (connRes.done || !connRes.value || connRes.value[1] !== 0x00) throw new Error("پـروکـسـی وصل شد اما دسترسی به اینترنت آزاد ندارد");*  
  
		*if (initialData &amp;amp;&amp;amp; initialData.byteLength &amp;gt; 0) {*  
  
			*await writer.write(convertToUint8Array(initialData));*  
  
		*}*  
  
		*writer.releaseLock();*  
  
		*reader.releaseLock();*  
  
		*return socket;*  
  
	*} catch (e) {*  
  
		*try {*  
  
			*writer.releaseLock();*  
  
		*} catch (err) {}*  
  
		*try {*  
  
			*reader.releaseLock();*  
  
		*} catch (err) {}*  
  
		*try {*  
  
			*socket.close();*  
  
		*} catch (err) {}*  
  
		*throw e;*  
  
	*}*  
  
*}*  
  
*async function connectHttp(proxyStr, destAddr, destPort, initialData) {*  
  
	*const { user, pass, host, port, auth } = parseProxyConfig(proxyStr, 80);*  
  
	*const socket = connect({ hostname: host, port: port });*  
  
	*const reader = socket.readable.getReader();*  
  
	*const writer = socket.writable.getWriter();*  
  
	*try {*  
  
		*const safeDest = destAddr.includes(":") ?* `[${destAddr}]` *: destAddr;*  
  
		*let req =* `CONNECT ${safeDest}:${destPort} HTTP/1.1\r\nHost: ${safeDest}:${destPort}\r\n`*;*  
  
		*if (auth) {*  
  
			*const authBase64 = bto*`${user}:${pass}`*);*  
  
			*req +=* `Proxy-Authorization: Basic ${authBase64}\r\n`*;*  
  
		*}*  
  
		*req += "\r\n";*  
  
		*await writer.write(new TextEncoder().encode(req));*  
  
		*let resStr = "";*  
  
		*while (true) {*  
  
			*const res = await [[reader.read](http://reader.read)]([http://reader.read)()](http://reader.read)());*  
  
			*if (res.done || !res.value) throw new Error("proxy_closed");*  
  
			*resStr += new TextDecoder().decode(res.value, { stream: true });*  
  
			*if (resStr.includes("\r\n\r\n")) {*  
  
				*const match = resStr.match(/^HTTP\/\d\.\d\s+(\d+)/);*  
  
				*if (match &amp;amp;&amp;amp; match[1] === "200") {*  
  
					*break;*  
  
				*} else {*  
  
					*throw new Error("proxy_error_" + (match ? match[1] : "unknown"));*  
  
				*}*  
  
			*}*  
  
		*}*  
  
		*if (initialData &amp;amp;&amp;amp; initialData.byteLength &amp;gt; 0) {*  
  
			*await writer.write(convertToUint8Array(initialData));*  
  
		*}*  
  
		*writer.releaseLock();*  
  
		*reader.releaseLock();*  
  
		*return socket;*  
  
	*} catch (e) {*  
  
		*try {*  
  
			*writer.releaseLock();*  
  
		*} catch (err) {}*  
  
		*try {*  
  
			*reader.releaseLock();*  
  
		*} catch (err) {}*  
  
		*try {*  
  
			*socket.close();*  
  
	*} catch (err) {}*  
  
		*throw e;*  
  
	*}*  
  
*}*  
  
*async function handleRailwayWS(request, env, ctx, userUuid) {*  
  
	*const user = await env.DB.prepare("SELECT username, is_active, limit_gb, used_gb, limit_req, used_req FROM users WHERE uuid = ?").bind(userUuid).first();*  
  
	*if (!user || [[user.is](http://user.is)]([http://user.is)_active](http://user.is)_active) === 0) {*  
  
		*return new Response("Access Denied", { status: 403 });*  
  
	*}*  
  
	*const username = user.username;*  
  
	*const cachedBytes = GLOBAL_TRAFFIC_CACHE.get(username) || 0;*  
  
	*if (user.limit_gb &amp;amp;&amp;amp; ((user.used_gb || 0) + (cachedBytes / (1024*  1024*  1024))) &amp;gt;= user.limit_gb) {  
  
		return new Response("Volume Exceeded", { status: 403 });  
  
	}  
  
	const cachedReqs = USER_REQ_CACHE.get(username) || 0;  
  
	if (user.limit_req &amp;amp;&amp;amp; ((user.used_req || 0) + cachedReqs) &amp;gt;= user.limit_req) {  
  
		return new Response("Request Limit Exceeded", { status: 403 });  
  
	}  
  
	USER_REQ_CACHE.set(username, cachedReqs + 1);  
  
	let activeCount = ACTIVE_CONNECTIONS_COUNT.get(username) || 0;  
  
	ACTIVE_CONNECTIONS_COUNT.set(username, activeCount + 1);  
  
	function addBytes(bytes) {  
  
		if (bytes &amp;lt;= 0) return;  
  
		let current = GLOBAL_TRAFFIC_CACHE.get(username) || 0;  
  
		GLOBAL_TRAFFIC_CACHE.set(username, current + bytes);  
  
		GLOBAL_LAST_ACTIVE_WRITE.set(username, [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)()));  
  
		if (GLOBAL_WRITE_LOCK.get(username)) return;  
  
		let lastDbWrite = GLOBAL_LAST_DB_WRITE.get(username) || 0;  
  
		let now = [[Date.now](http://Date.now)]([http://Date.now)()](http://Date.now)());  
  
		let thresholdBytes = 10  *1024*  1024;  
  
		if (current &amp;gt;= thresholdBytes || (current &amp;gt; 0 &amp;amp;&amp;amp; now - lastDbWrite &amp;gt; 60000)) {  
  
			GLOBAL_WRITE_LOCK.set(username, true);  
  
			let toCommit = GLOBAL_TRAFFIC_CACHE.get(username) || 0;  
  
			let toCommitReq = USER_REQ_CACHE.get(username) || 0;  
  
			if (toCommit &amp;lt;= 0 &amp;amp;&amp;amp; toCommitReq &amp;lt;= 0) {  
  
				GLOBAL_WRITE_LOCK.set(username, false);  
  
				return;  
  
			}  
  
			GLOBAL_TRAFFIC_CACHE.set(username, 0);  
  
			USER_REQ_CACHE.set(username, 0);  
  
			GLOBAL_LAST_DB_WRITE.set(username, now);  
  
			let deltaGb = toCommit / (1024  *1024*  1024);  
  
			env.DB.prepare("UPDATE users SET used_gb = used_gb + ?, used_req = used_req + ? WHERE username = ?")  
  
				.bind(deltaGb, toCommitReq, username)  
  
				.run()  
  
				.catch(e =&amp;gt; console.error(e))  
  
				.finally(() =&amp;gt; GLOBAL_WRITE_LOCK.set(username, false));  
  
		}  
  
	}  
  
	const newHeaders = new Headers(request.headers);  
  
	let railwayResponse = null;  
  
	for (const backend of RAILWAY_BACKENDS) {  
  
		try {  
  
			const railwayUrl = new URL(request.url);  
  
			railwayUrl.hostname = backend;  
  
			railwayUrl.pathname = "/Ma_Ke_Vaslim";  
  
			railwayUrl.protocol = "https:";  
  
			railwayUrl.port = "443";  
  
			newHeaders.set("Host", backend);  
  
			const res = await fetch(railwayUrl.toString(), {  
  
				method: "GET",  
  
				headers: newHeaders,  
  
				redirect: "manual"  
  
			});  
  
			if (res.webSocket) {  
  
				railwayResponse = res;  
  
				break;  
  
			}  
  
		} catch (err) {  
  
			console.lo`Backend ${backend} failed, trying next...`);  
  
		}  
  
	}  
  
	if (!railwayResponse || !railwayResponse.webSocket) {  
  
		let ac = ACTIVE_CONNECTIONS_COUNT.get(username) || 1;  
  
		ACTIVE_CONNECTIONS_COUNT.set(username, Math.max(0, ac - 1));  
  
		return new Response("All Railway Backends Offline", { status: 502 });  
  
	}  
  
	const backendSocket = railwayResponse.webSocket;  
  
	const { 0: clientSocket, 1: localServerSocket } = new WebSocketPair();  
  
	backendSocket.accept();  
  
	localServerSocket.accept();  
  
	const createWsStream = (ws) =&amp;gt; {  
  
		const readable = new ReadableStream({  
  
			start(controller) {  
  
				ws.addEventListener("message", e =&amp;gt; controller.enqueue([[e.data](http://e.data)]([http://e.data](http://e.data))));  
  
				ws.addEventListener("close", () =&amp;gt; controller.close());  
  
				ws.addEventListener("error", e =&amp;gt; controller.error(e));  
  
			},  
  
			cancel() { ws.close(); }  
  
		});  
  
		const writable = new WritableStream({  
  
			write(chunk) {  
  
				if (ws.readyState === 1) ws.send(chunk);  
  
			},  
  
			close() { ws.close(); },  
  
			abort() { ws.close(); }  
  
		});  
  
		return { readable, writable };  
  
	};  
  
	const localStream = createWsStream(localServerSocket);  
  
	const remoteStream = createWsStream(backendSocket);  
  
	const createTrafficCounter = () =&amp;gt; new TransformStream({  
  
		transform(chunk, controller) {  
  
			const bytes = chunk.byteLength || chunk.length || 0;  
  
			addBytes(bytes);  
  
			controller.enqueue(chunk);  
  
		}  
  
	});  
  
	localStream.readable  
  
		.pipeThrough(createTrafficCounter())  
  
		.pipeTo(remoteStream.writable)  
  
		.catch(() =&amp;gt; {});  
  
	remoteStream.readable  
  
		.pipeThrough(createTrafficCounter())  
  
		.pipeTo(localStream.writable)  
  
		.catch(() =&amp;gt; {});  
  
	let isClosed = false;  
  
	const closeSockets = () =&amp;gt; {  
  
		if(isClosed) return;  
  
		isClosed = true;  
  
		try { localServerSocket.close(); } catch(e) {}  
  
		try { backendSocket.close(); } catch(e) {}  
  
		let ac = ACTIVE_CONNECTIONS_COUNT.get(username) || 1;  
  
		ac = Math.max(0, ac - 1);  
  
		ACTIVE_CONNECTIONS_COUNT.set(username, ac);  
  
		if (ac === 0) {  
  
			let remainingBytes = GLOBAL_TRAFFIC_CACHE.get(username) || 0;  
  
			let remainingReqs = USER_REQ_CACHE.get(username) || 0;  
  
			if (remainingBytes &amp;gt; 0 || remainingReqs &amp;gt; 0) {  
  
				GLOBAL_WRITE_LOCK.set(username, true);  
  
				let deltaGb = remainingBytes / (1024  *1024*  1024);  
  
				const dbTask = env.DB.prepare("UPDATE users SET used_gb = used_gb + ?, used_req = used_req + ? WHERE username = ?")  
  
					.bind(deltaGb, remainingReqs, username)  
  
					.run()  
  
					.catch(e =&amp;gt; console.error("DB Save Error:", e))  
  
					.finally(() =&amp;gt; {  
  
						GLOBAL_WRITE_LOCK.delete(username);  
  
						GLOBAL_TRAFFIC_CACHE.delete(username);  
  
						USER_REQ_CACHE.delete(username);  
  
					});  
  
				if (ctx) ctx.waitUntil(dbTask);  
  
				else dbTask;  
  
			} else {  
  
				GLOBAL_TRAFFIC_CACHE.delete(username);  
  
				USER_REQ_CACHE.delete(username);  
  
			}  
  
		}  
  
	};  
  
	localServerSocket.addEventListener("close", closeSockets);  
  
	backendSocket.addEventListener("close", closeSockets);  
  
	localServerSocket.addEventListener("error", closeSockets);  
  
	backendSocket.addEventListener("error", closeSockets);  
  
	return new Response(null, {  
  
		status: 101,  
  
		webSocket: clientSocket  
  
	});  
  
}  
  
const COMMON_HEAD = `&lt;script src="[https://cdn.tailwindcss.com"&gt;&lt;/script&gt;](https://cdn.tailwindcss.com"></script>)`  
  
`&lt;script src="[https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"&gt;&lt;/script&gt;](https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>)`  
  
`&lt;link href="[https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css](https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css)" rel="stylesheet" type="text/css" /&gt;`  
  
`&lt;script&gt;`  
  
	`tailwind.config = {`  
  
		`darkMode: 'class',`  
  
		`theme: {`  
  
			`extend: {`  
  
				`fontFamily: { sans: ['Vazirmatn', 'sans-serif'] },`  
  
				`colors: { amoled: { bg: '#000105', card: '#040914', input: '#081224', border: '#102040' } }`  
  
			`}`  
  
		`}`  
  
	`}`  
  
`&lt;/script&gt;`;  
  
const COMMON_TOAST_HTML = `<div id="toast-container" class="fixed top-5 left-1/2 -translate-x-1/2 z-[9999] flex flex-col gap-2 pointer-events-none"></div>`;  
  
const COMMON_TOAST_JS =   
  
		`function showToast(message, type = 'success') {`  
  
			`const container = document.getElementById('toast-container');`  
  
			`const toast = document.createElement('div');`  
  
			`const colors = type === 'error' `  
  
				`? 'bg-red-50 dark:bg-red-900/40 border-red-200 dark:border-red-800 text-red-600 dark:text-red-400' `  
  
				`: 'bg-green-50 dark:bg-green-900/40 border-green-200 dark:border-green-800 text-green-700 dark:text-green-500';`  
  
			`toast.className = 'px-4 py-3 border rounded-md shadow-lg font-bold text-sm transform transition-all duration-300 -translate-y-full opacity-0 ' + colors;`  
  
			`toast.innerText = message;`  
  
			`container.appendChild(toast);`  
  
			`requestAnimationFrame(() =&gt; {`  
  
				`toast.classList.remove('-translate-y-full', 'opacity-0');`  
  
			`});`  
  
			`setTimeout(() =&gt; {`  
  
				`toast.classList.add('-translate-y-full', 'opacity-0');`  
  
				`setTimeout(() =&gt; toast.remove(), 300);`  
  
            `}, 3000);`  
  
        `}`  
  
        `window.alert = function(message) {`  
  
            `const msgStr = message ? message.toString() : '';`  
  
            `if (msgStr.includes('خطا') || msgStr.includes('⚠️') || msgStr.includes('❌')) {`  
  
                `showToast(msgStr, 'error');`  
  
            `} else {`  
  
                `showToast(msgStr, 'success');`  
  
            `}`  
  
        `};`  
  
;  
  
const HTML_TEMPLATES = {  
  
	nginx: `&lt;!DOCTYPE html&gt;`  
  
`&lt;html lang="fa" dir="rtl" class="dark"&gt;`  
  
`&lt;head&gt;`  
  
    `&lt;meta charset="UTF-8"&gt;`  
  
    `&lt;meta name="viewport" content="width=device-width, initial-scale=1.0"&gt;`  
  
    `&lt;title&gt;دسترسی به پـنـل&lt;/title&gt;`  
  
    `${COMMON_HEAD}`  
  
`&lt;/head&gt;`  
  
`&lt;body class="bg-gray-50 text-gray-900 dark:bg-amoled-bg dark:text-zinc-100 min-h-screen flex items-center justify-center p-4"&gt;`  
  
    `&lt;div class="w-full max-w-md bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md shadow-xl p-8 text-center flex flex-col items-center gap-4"&gt;`  
  
        `&lt;div class="p-4 bg-blue-50 dark:bg-blue-900/20 text-blue-500 rounded-full mb-2"&gt;`  
  
            `&lt;svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;h2 class="text-xl font-bold text-gray-900 dark:text-white"&gt;ورود به پـنـل مدیریت&lt;/h2&gt;`  
  
        `&lt;p class="text-sm text-gray-600 dark:text-gray-400 leading-relaxed mt-2"&gt;`  
  
            `برای ورود به پـنـل، لطفاً عبارت `  
  
            `&lt;span class="inline-block px-2 py-1 bg-gray-100 dark:bg-amoled-input border border-gray-200 dark:border-zinc-800 rounded-md font-mono text-blue-500 font-bold mx-1 shadow-sm" dir="ltr"&gt;/panel&lt;/span&gt; `  
  
            `را به انتهای آدرس مرورگر خود اضافه کنید.`  
  
        `&lt;/p&gt;`  
  
        `&lt;button onclick="window.location.href='/panel'" class="mt-4 w-full py-2.5 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-medium rounded-md text-sm transition-colors duration-200 shadow-lg font-bold"&gt;`  
  
            `ورود به پـنـل`  
  
        `&lt;/button&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;/body&gt;`  
  
`&lt;/html&gt;`,  
  
	setup: `&lt;!DOCTYPE html&gt;`  
  
`&lt;html lang="fa" dir="rtl" class="dark"&gt;`  
  
`&lt;head&gt;`  
  
    `&lt;meta charset="UTF-8"&gt;`  
  
    `&lt;meta name="viewport" content="width=device-width, initial-scale=1.0"&gt;`  
  
    `&lt;title&gt;تعریف رمز عبور پـنـل&lt;/title&gt;`  
  
    `${COMMON_HEAD}`  
  
`&lt;/head&gt;`  
  
`&lt;body class="bg-gray-50 text-gray-900 dark:bg-amoled-bg dark:text-zinc-100 min-h-screen flex items-center justify-center p-4"&gt;`  
  
    `&lt;div class="w-full max-w-md bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md shadow-xl p-6"&gt;`  
  
        `&lt;h2 class="text-xl font-bold mb-2 text-center text-blue-600 dark:text-blue-400"&gt;تنظیم رمز عبور جدید&lt;/h2&gt;`  
  
        `&lt;p class="text-sm text-gray-500 dark:text-gray-400 text-center mb-6"&gt;این اولین ورود شما به پـنـل مدیریت است. لطفاً رمز عبور خود را تعیین کنید.&lt;/p&gt;`  
  
        `&lt;form onsubmit="handleSetup(event)" class="space-y-4"&gt;`  
  
            `&lt;div&gt;`  
  
                `&lt;label class="block text-sm font-medium mb-1.5"&gt;رمز عبور&lt;/label&gt;`  
  
                `&lt;input type="password" id="password" class="w-full px-3 py-2 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm text-center font-mono" required minlength="4"&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;div&gt;`  
  
                `&lt;label class="block text-sm font-medium mb-1.5"&gt;تکرار رمز عبور&lt;/label&gt;`  
  
                `&lt;input type="password" id="confirm-password" class="w-full px-3 py-2 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm text-center font-mono" required minlength="4"&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;button type="submit" id="submit-btn" class="w-full py-2.5 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-medium rounded-md text-sm transition font-bold"&gt;ثبت و ورود&lt;/button&gt;`  
  
        `&lt;/form&gt;`  
  
    `&lt;/div&gt;`  
  
    `${COMMON_TOAST_HTML}`  
  
    `&lt;script&gt;`  
  
        `${COMMON_TOAST_JS};`  
  
        `async function handleSetup(event) {`  
  
            `event.preventDefault();`  
  
            `const password = document.getElementById('password').value;`  
  
            `const confirmPassword = document.getElementById('confirm-password').value;`  
  
            `const btn = document.getElementById('submit-btn');`  
  
            `if (password !== confirmPassword) {`  
  
                `alert('⚠️ رمز عبور و تکرار آن مطابقت ندارند!');`  
  
                `return;`  
  
            `}`  
  
            `btn.disabled = true;`  
  
            `btn.innerText = 'در حال ثبت...';`  
  
            `try {`  
  
                `const res = await fetch('/api/setup-password', {`  
  
                    `method: 'POST',`  
  
                    `headers: { 'Content-Type': 'application/json' },`  
  
                    `body: JSON.stringify({ password })`  
  
                `});`  
  
                `const data = await res.json();`  
  
                `if (res.ok &amp;&amp; data.success) {`  
  
                    `alert('✅ رمز عبور با موفقیت تنظیم شد. در حال ورود...');`  
  
                    `setTimeout(() =&gt; {`  
  
                        `window.location.reload();`  
  
                    `}, 1500);`  
  
                `} else {`  
  
                    `alert('خطا: ' + (data.error || 'عملیات ناموفق بود'));`  
  
                `}`  
  
            `} catch (err) {`  
  
                `alert('خطا در ارتباط با سرور');`  
  
            `} finally {`  
  
                `btn.disabled = false;`  
  
                `btn.innerText = 'ثبت و ورود';`  
  
            `}`  
  
        `}`  
  
    `&lt;/script&gt;`  
  
`&lt;/body&gt;`  
  
`&lt;/html&gt;`,  
  
	login: `&lt;!DOCTYPE html&gt;`  
  
`&lt;html lang="fa" dir="rtl" class="dark"&gt;`  
  
`&lt;head&gt;`  
  
    `&lt;meta charset="UTF-8"&gt;`  
  
    `&lt;meta name="viewport" content="width=device-width, initial-scale=1.0"&gt;`  
  
    `&lt;title&gt;ورود به پـنـل مدیریت&lt;/title&gt;`  
  
    `${COMMON_HEAD}`  
  
`&lt;/head&gt;`  
  
`&lt;body class="bg-gray-50 text-gray-900 dark:bg-amoled-bg dark:text-zinc-100 min-h-screen flex items-center justify-center p-4"&gt;`  
  
    `&lt;div class="w-full max-w-md bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md shadow-xl p-6"&gt;`  
  
        `&lt;div id="login-section"&gt;`  
  
            `&lt;h2 class="text-xl font-bold mb-6 text-center text-blue-600 dark:text-blue-400"&gt;ورود به پـنـل مدیریت&lt;/h2&gt;`  
  
            `&lt;form onsubmit="handleLogin(event)" class="space-y-4"&gt;`  
  
                `&lt;div&gt;`  
  
                    `&lt;label class="block text-sm font-medium mb-1.5"&gt;رمز عبور&lt;/label&gt;`  
  
                    `&lt;input type="password" id="password" class="w-full px-3 py-2 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm text-center font-mono" required&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;button type="submit" id="submit-btn" class="w-full py-2.5 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-medium rounded-md text-sm transition font-bold"&gt;ورود&lt;/button&gt;`  
  
            `&lt;/form&gt;`  
  
            `&lt;div class="mt-4 text-center"&gt;`  
  
                `&lt;button onclick="toggleRecovery(true)" class="text-xs text-blue-500 hover:text-blue-600 transition font-medium"&gt;بازیابی رمز پـنـل&lt;/button&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;div id="recovery-section" class="hidden"&gt;`  
  
            `&lt;h2 class="text-xl font-bold mb-4 text-center text-orange-600 dark:text-orange-400"&gt;بازیابی رمز پـنـل&lt;/h2&gt;`  
  
            `&lt;div class="mb-5 p-3 bg-orange-50 dark:bg-orange-900/20 border border-orange-200 dark:border-orange-800/50 rounded-md text-xs leading-relaxed text-orange-800 dark:text-orange-300"&gt;`  
  
                `برای احراز هویت و اثبات مالکیت پـنـل، از طریق دکمه زیر وارد کلودفلر شوید و توکن دریافتی را کپی کرده و در کادر زیر وارد کنید.`  
  
                `&lt;a href="[https://dash.cloudflare.com/profile/api-tokens?permissionGroupKeys=%5B%7B%22key%22%3A%22workers_scripts%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22workers_kv_storage%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22d1%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22account_settings%22%2C%22type%22%3A%22read%22%7D%2C%7B%22key%22%3A%22workers_subdomain%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22account_analytics%22%2C%22type%22%3A%22read%22%7D%5D&amp;accountId=*&amp;zoneId=all&amp;name=Zeus-Deployer-Token](https://dash.cloudflare.com/profile/api-tokens?permissionGroupKeys=%5B%7B%22key%22%3A%22workers_scripts%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22workers_kv_storage%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22d1%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22account_settings%22%2C%22type%22%3A%22read%22%7D%2C%7B%22key%22%3A%22workers_subdomain%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22account_analytics%22%2C%22type%22%3A%22read%22%7D%5D&accountId=*&zoneId=all&name=Zeus-Deployer-Token)" target="_blank" class="mt-3 w-full flex items-center justify-center gap-2 py-2 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 rounded-md font-bold transition shadow-md"&gt;`  
  
                    `&lt;svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                    `دریافت توکن`  
  
                `&lt;/a&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;form onsubmit="handleRecovery(event)" class="space-y-4"&gt;`  
  
                `&lt;div&gt;`  
  
                    `&lt;input type="password" id="api-token" placeholder="توکن را وارد کنید" class="w-full px-3 py-2 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-orange-500 text-xs text-center font-mono" required&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;div class="flex gap-2 pt-2"&gt;`  
  
                    `&lt;button type="button" onclick="toggleRecovery(false)" class="w-1/3 py-2.5 bg-transparent border-2 border-rose-700 text-rose-700 hover:bg-rose-900/20 hover:text-rose-800 dark:border-rose-700 dark:text-rose-500 dark:hover:bg-rose-900/40 dark:hover:text-rose-400 font-bold rounded-md text-sm transition shadow-sm"&gt;انصراف&lt;/button&gt;`  
  
                    `&lt;button type="submit" id="recover-btn" class="w-2/3 py-2.5 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-medium rounded-md text-sm transition font-bold"&gt;بازیابی رمز پـنـل&lt;/button&gt;`  
  
                `&lt;/div&gt;`  
  
            `&lt;/form&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
    `${COMMON_TOAST_HTML}`  
  
    `&lt;script&gt;`  
  
        `${COMMON_TOAST_JS}`  
  
        `async function handleLogin(event) {`  
  
            `event.preventDefault();`  
  
            `const password = document.getElementById('password').value;`  
  
            `const btn = document.getElementById('submit-btn');`  
  
            `btn.disabled = true;`  
  
            `try {`  
  
                `const res = await fetch('/api/login', {`  
  
                    `method: 'POST',`  
  
                    `headers: { 'Content-Type': 'application/json' },`  
  
                    `body: JSON.stringify({ password })`  
  
                `});`  
  
                `const data = await res.json();`  
  
                `if (res.ok &amp;&amp; data.success) {`  
  
                    `window.location.reload();`  
  
                `} else {`  
  
                    `alert('❌ رمز عبور اشتباه است');`  
  
                `}`  
  
            `} catch (err) {`  
  
                `alert('خطا در ارتباط با سرور');`  
  
            `} finally {`  
  
                `btn.disabled = false;`  
  
            `}`  
  
        `}`  
  
        `function toggleRecovery(show) {`  
  
            `document.getElementById('login-section').classList.toggle('hidden', show);`  
  
            `document.getElementById('recovery-section').classList.toggle('hidden', !show);`  
  
        `}`  
  
        `async function handleRecovery(event) {`  
  
            `event.preventDefault();`  
  
            `const apiToken = document.getElementById('api-token').value;`  
  
            `const btn = document.getElementById('recover-btn');`  
  
            `btn.disabled = true;`  
  
            `btn.innerText = 'در حال بررسی...';`  
  
            `try {`  
  
                `const res = await fetch('/api/recover', {`  
  
                    `method: 'POST',`  
  
                    `headers: { 'Content-Type': 'application/json' },`  
  
                    `body: JSON.stringify({ api_token: apiToken })`  
  
                `});`  
  
                `const data = await res.json();`  
  
                `if (res.ok &amp;&amp; data.success) {`  
  
                    `alert('✅ رمز عبور با موفقیت حذف شد. در حال انتقال به صفحه تنظیمات اولیه...');`  
  
                    `setTimeout(() =&gt; {`  
  
                        `window.location.reload();`  
  
                    `}, 1500);`  
  
                `} else {`  
  
                    `alert('❌ ' + (data.error || 'خطا در تایید اطلاعات'));`  
  
                `}`  
  
            `} catch (err) {`  
  
                `alert('خطا در ارتباط با سرور');`  
  
            `} finally {`  
  
                `btn.disabled = false;`  
  
                `btn.innerText = 'بازیابی رمز پـنـل';`  
  
            `}`  
  
        `}`  
  
    `&lt;/script&gt;`  
  
`&lt;/body&gt;`  
  
`&lt;/html&gt;`,  
  
	panel:   
  
`&lt;!DOCTYPE html&gt;`  
  
`&lt;html lang="fa" dir="rtl"&gt;`  
  
`&lt;head&gt;`  
  
    `&lt;meta charset="UTF-8"&gt;`  
  
    `&lt;meta name="viewport" content="width=device-width, initial-scale=1.0"&gt;`  
  
    `&lt;title&gt;Ma Ke Vaslim&lt;/title&gt;`  
  
    `&lt;script&gt;`  
  
        `const originalWarn = console.warn;`  
  
        `console.warn = (...args) =&gt; {`  
  
            `if (typeof args[0] === 'string' &amp;&amp; args[0].includes('[cdn.tailwindcss.com](http://cdn.tailwindcss.com)')) return;`  
  
            `originalWarn(...args);`  
  
        `};`  
  
    `&lt;/script&gt;`  
  
    `${COMMON_HEAD}`  
  
    `&lt;style&gt;`  
  
        `body { font-family: 'Vazirmatn', sans-serif; }`  
  
		`.dark input[type="checkbox"] {`  
  
            `filter: invert(1) hue-rotate(180deg);`  
  
        `}`  
  
        `::-webkit-scrollbar {`  
  
            `width: 6px;`  
  
            `height: 6px;`  
  
        `}`  
  
        `::-webkit-scrollbar-track {`  
  
            `background: #f3f4f6; `  
  
            `border-radius: 4px;`  
  
        `}`  
  
        `::-webkit-scrollbar-thumb {`  
  
            `background: #d1d5db; `  
  
            `border-radius: 4px;`  
  
        `}`  
  
        `::-webkit-scrollbar-thumb:hover {`  
  
            `background: #9ca3af;`  
  
        `}`  
  
        `.dark ::-webkit-scrollbar-track {`  
  
            `background: #000105; `  
  
        `}`  
  
        `.dark ::-webkit-scrollbar-thumb {`  
  
            `background: #102040; `  
  
        `}`  
  
        `.dark ::-webkit-scrollbar-thumb:hover {`  
  
            `background: #172e5c;`  
  
        `}`  
  
         `{`  
  
            `scrollbar-width: thin;`  
  
            `scrollbar-color: #d1d5db #f3f4f6;`  
  
        `}`  
  
        `.dark  {`  
  
            `scrollbar-color: #102040 #000105;`  
  
        `}`  
  
        `@media (min-width: 769px) {`  
  
            `header, main { zoom: 1.18; }`  
  
        `}`  
  
        `@media (max-width: 768px) {`  
  
            `header, main { zoom: 0.90; }`  
  
        `}`  
  
        `input[type="number"]::-webkit-outer-spin-button,`  
  
        `input[type="number"]::-webkit-inner-spin-button {`  
  
            `-webkit-appearance: none;`  
  
            `margin: 0;`  
  
        `}`  
  
        `input[type="number"] {`  
  
            `-moz-appearance: textfield;`  
  
        `}`  
  
    `&lt;/style&gt;`  
  
`&lt;/head&gt;`  
  
`&lt;body class="bg-gray-50 text-gray-900 dark:bg-amoled-bg dark:text-zinc-100 min-h-screen transition-colors duration-200"&gt;`  
  
    `&lt;header class="border-b border-gray-200 dark:border-amoled-border bg-white dark:bg-amoled-card px-4 py-4"&gt;`  
  
        `&lt;div class="max-w-6xl mx-auto flex flex-col md:flex-row justify-between items-center gap-4"&gt;`  
  
            `&lt;div class="flex flex-row flex-wrap justify-center items-center gap-3 w-full md:w-auto"&gt;`  
  
                `&lt;h1 class="text-lg font-bold flex items-center gap-2" dir="ltr"&gt;`  
  
                    `Ma Ke Vaslim`  
  
                    `&lt;span id="panel-version" class="text-xs px-2 py-0.5 font-semibold bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 rounded-full"&gt;v2.0.0&lt;/span&gt;`  
  
                `&lt;/h1&gt;`  
  
                `&lt;div class="flex items-center gap-3 bg-gray-100 dark:bg-zinc-800/60 px-3 py-1.5 rounded-full border border-gray-200 dark:border-zinc-800/80 shadow-sm flex-shrink-0 w-fit"&gt;`  
  
                    `&lt;a href="[https://github.com/mkvaslim44/Ma_ke_vaslim-Panel](https://github.com/mkvaslim44/Ma_ke_vaslim-Panel)" target="_blank" rel="noopener noreferrer" class="text-gray-600 hover:text-gray-800 dark:text-gray-400 dark:hover:text-gray-200 transition-all transform hover:scale-125 duration-200 flex-shrink-0" title="GitHub"&gt;`  
  
                        `&lt;svg class="w-[22px] h-[22px] flex-shrink-0" viewBox="0 0 24 24" fill="currentColor"&gt;`  
  
                            `&lt;path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12"/&gt;`  
  
                        `&lt;/svg&gt;`  
  
                    `&lt;/a&gt;`  
  
                    `&lt;a href="[https://t.me/makevaslim4](https://t.me/makevaslim4)" target="_blank" rel="noopener noreferrer" class="text-sky-500 hover:text-sky-600 dark:hover:text-sky-400 transition-all transform hover:scale-125 duration-200 flex-shrink-0" title="Telegram"&gt;`  
  
                        `&lt;svg class="w-[22px] h-[22px] flex-shrink-0" viewBox="0 0 24 24" fill="currentColor"&gt;`  
  
                            `&lt;path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-1.13 7.19-.14.75-.42 1-.68 1.03-.58.05-1.02-.38-1.58-.75-.88-.58-1.38-.94-2.23-1.5-.99-.65-.35-1.01.22-1.59.15-.15 2.71-2.48 2.76-2.69a.2.2 0 00-.05-.18c-.06-.05-.14-.03-.21-.02-.09.02-1.49.94-4.22 2.79-.4.27-.76.41-1.08.4-.36-.01-1.04-.2-1.55-.37-.63-.2-1.12-.31-1.08-.66.02-.18.27-.36.74-.55 2.92-1.27 4.86-2.11 5.83-2.51 2.78-1.16 3.35-1.36 3.73-1.37.08 0 .27.02.39.12.1.08.13.19.14.27-.01.06.01.24 0 .24z"/&gt;`  
  
                        `&lt;/svg&gt;`  
  
                    `&lt;/a&gt;`  
  
                    `&lt;a href="[https://t.me/makevaslim4](https://t.me/makevaslim4)" target="_blank" rel="noopener noreferrer" class="text-emerald-500 hover:text-emerald-600 dark:hover:text-emerald-400 transition-all transform hover:scale-125 duration-200 flex-shrink-0" title="Bot"&gt;`  
  
                        `&lt;svg class="w-[22px] h-[22px] flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"&gt;`  
  
                            `&lt;path d="M12 8V4H8"/&gt;`  
  
                            `&lt;rect width="16" height="12" x="4" y="8" rx="2"/&gt;`  
  
                            `&lt;path d="M2 14h2"/&gt;`  
  
                            `&lt;path d="M20 14h2"/&gt;`  
  
                            `&lt;path d="M15 13v2"/&gt;`  
  
                            `&lt;path d="M9 13v2"/&gt;`  
  
                        `&lt;/svg&gt;`  
  
                    `&lt;/a&gt;`  
  
                `&lt;/div&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;div class="flex items-center justify-center gap-3 w-full md:w-auto mt-2 md:mt-0"&gt;`  
  
                `&lt;button onclick="toggleSupportModal(true)" `  
  
                        `class="p-2 rounded-md `  
  
                               `bg-red-50 dark:bg-red-950/30 `  
  
                               `border border-red-200 dark:border-red-900 `  
  
                               `hover:bg-red-100 dark:hover:bg-red-900/50 `  
  
                               `transition-all duration-200 `  
  
                               `text-red-600 dark:text-red-400 shadow-sm" `  
  
                        `title="حمایت از ما"&gt;`  
  
                    `&lt;svg class="w-5 h-5 animate-pulse" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"&gt;`  
  
                        `&lt;path stroke-linecap="round" stroke-linejoin="round" d="M21 8.25c0-2.485-2.099-4.5-4.688-4.5-1.935 0-3.597 1.126-4.312 2.733-.715-1.607-2.377-2.733-4.313-2.733C5.1 3.75 3 5.765 3 8.25c0 7.22 9 12 9 12s9-4.78 9-12z" /&gt;`  
  
                    `&lt;/svg&gt;`  
  
                `&lt;/button&gt;`  
  
				`&lt;button onclick="restartCore()"`  
  
                        `class="p-2 rounded-md `  
  
                               `bg-blue-50 dark:bg-blue-950/30 `  
  
                               `border border-blue-200 dark:border-blue-900 `  
  
                               `hover:bg-blue-100 dark:hover:bg-blue-900/50 `  
  
                               `transition-all duration-200 `  
  
                               `text-blue-600 dark:text-blue-400 shadow-sm" `  
  
                        `title="ری استارت پـنـل"&gt;`  
  
                    `&lt;svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;`  
  
                        `&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"&gt;&lt;/path&gt;`  
  
                    `&lt;/svg&gt;`  
  
                `&lt;/button&gt;`  
  
                `&lt;button id="theme-toggle" `  
  
                        `class="p-2 rounded-md `  
  
                               `bg-amber-50 dark:bg-amber-950/30 `  
  
                               `border border-amber-200 dark:border-amber-900 `  
  
                               `hover:bg-amber-100 dark:hover:bg-amber-900/50 `  
  
                               `transition-all duration-200 `  
  
                               `text-amber-500 dark:text-amber-400 shadow-sm"`  
  
                        `title="تغییر تم"&gt;`  
  
                    `&lt;svg id="sun-icon" class="w-5 h-5 hidden dark:block" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;`  
  
                        `&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364-6.364l-.707.707M6.343 17.657l-.707.707m12.728 0l-.707-.707M6.343 6.343l-.707-.707M14 12a2 2 0 11-4 0 2 2 0 014 0z"&gt;&lt;/path&gt;`  
  
                    `&lt;/svg&gt;`  
  
                    `&lt;svg id="moon-icon" class="w-5 h-5 block dark:hidden" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;`  
  
                        `&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"&gt;&lt;/path&gt;`  
  
                    `&lt;/svg&gt;`  
  
                `&lt;/button&gt;`  
  
                `&lt;button id="update-toggle" onclick="checkForUpdates(true)" `  
  
                        `class="p-2 rounded-md `  
  
                               `bg-green-50 dark:bg-green-950/30 `  
  
                               `border border-green-200 dark:border-green-900 `  
  
                               `hover:bg-green-100 dark:hover:bg-green-900/50 `  
  
                               `transition-all duration-200 `  
  
                               `text-green-700 dark:text-green-500 `  
  
                               `relative shadow-sm" `  
  
                        `title="آپدیت"&gt;`  
  
                    `&lt;svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;`  
  
                        `&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 11l3-3m0 0l3 3m-3-3v8m0-13a9 9 0 110 18 9 9 0 010-18z"&gt;&lt;/path&gt;`  
  
                    `&lt;/svg&gt;`  
  
                    `&lt;span id="update-badge" class="absolute top-0 right-0 w-2.5 h-2.5 bg-red-500 border-2 border-green-50 dark:border-green-900 rounded-full hidden animate-pulse"&gt;&lt;/span&gt;`  
  
                `&lt;/button&gt;`  
  
                `&lt;button onclick="toggleSettingsModal(true)" `  
  
                        `class="p-2 rounded-md `  
  
                               `bg-gray-50 dark:bg-zinc-800/50 `  
  
                               `border border-gray-200 dark:border-zinc-700 `  
  
                               `hover:bg-gray-100 dark:hover:bg-zinc-700/80 `  
  
                               `transition-all duration-200 `  
  
                               `text-gray-600 dark:text-zinc-400 shadow-sm" `  
  
                        `title="تنظیمات"&gt;`  
  
                    `&lt;svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;`  
  
                        `&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"&gt;&lt;/path&gt;`  
  
                        `&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"&gt;&lt;/path&gt;`  
  
                    `&lt;/svg&gt;`  
  
                `&lt;/button&gt;`  
  
                `&lt;button `  
  
                    `onclick="logoutAdmin()" `  
  
                    `class="p-2 rounded-md `  
  
                           `bg-red-50 dark:bg-red-950/30 `  
  
                           `border border-red-200 dark:border-red-900 `  
  
                           `hover:bg-red-100 dark:hover:bg-red-900/50 `  
  
                           `transition-all duration-200 `  
  
                           `text-red-600 dark:text-red-400 `  
  
                           `shadow-sm hover:shadow-md"`  
  
                    `title="خروج"&gt;`  
  
                    `&lt;svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;`  
  
                        `&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"&gt;&lt;/path&gt;`  
  
                    `&lt;/svg&gt;`  
  
                `&lt;/button&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/header&gt;`  
  
    `&lt;main class="max-w-6xl mx-auto px-4 py-8 pb-56 md:pb-32"&gt;`  
  
`&lt;div class="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-6"&gt;`  
  
    `&lt;div class="bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md p-2.5 shadow-sm flex flex-col justify-center gap-1 hover:shadow-md hover:border-indigo-400 dark:hover:border-indigo-500/50 transition duration-300 relative overflow-hidden group min-h-[64px]"&gt;`  
  
        `&lt;div class="absolute -right-4 -bottom-4 w-16 h-16 bg-indigo-500/10 rounded-full blur-xl group-hover:scale-150 transition duration-500"&gt;&lt;/div&gt;`  
  
        `&lt;div class="flex items-center justify-between relative z-10"&gt;`  
  
            `&lt;span class="text-[11px] sm:text-xs font-semibold text-gray-500 dark:text-zinc-400 whitespace-nowrap"&gt;تعداد کل کاربران&lt;/span&gt;`  
  
            `&lt;div class="p-1 bg-indigo-50 dark:bg-indigo-950/30 text-indigo-600 dark:text-indigo-400 rounded-md flex-shrink-0"&gt;`  
  
                `&lt;svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;div class="flex items-end justify-between relative z-10 w-full mt-0.5"&gt;`  
  
            `&lt;div class="text-lg font-black text-gray-900 dark:text-zinc-100 transition-all leading-none" id="stat-total-users"&gt;0&lt;/div&gt;`  
  
            `&lt;span class="text-[9px] text-indigo-500 dark:text-indigo-400 flex items-center gap-1 font-medium whitespace-nowrap leading-none mb-0.5"&gt;`  
  
                `&lt;span class="w-1 h-1 bg-indigo-500 rounded-full animate-ping"&gt;&lt;/span&gt;`  
  
                `کل کاربران تعریف شده`  
  
            `&lt;/span&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
    `&lt;div class="bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md p-2.5 shadow-sm flex flex-col justify-center gap-1 hover:shadow-md hover:border-emerald-400 dark:hover:border-emerald-500/50 transition duration-300 relative overflow-hidden group min-h-[64px]"&gt;`  
  
        `&lt;div class="absolute -right-4 -bottom-4 w-16 h-16 bg-emerald-500/10 rounded-full blur-xl group-hover:scale-150 transition duration-500"&gt;&lt;/div&gt;`  
  
        `&lt;div class="flex items-center justify-between relative z-10"&gt;`  
  
            `&lt;span class="text-[11px] sm:text-xs font-semibold text-gray-500 dark:text-zinc-400 whitespace-nowrap"&gt;کاربران فعال (آنلاین)&lt;/span&gt;`  
  
            `&lt;div class="p-1 bg-emerald-50 dark:bg-emerald-950/30 text-emerald-600 dark:text-emerald-400 rounded-md flex-shrink-0"&gt;`  
  
                `&lt;svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M13 10V3L4 14h7v7l9-11h-7z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;div class="flex items-end justify-between relative z-10 w-full mt-0.5"&gt;`  
  
            `&lt;div class="text-lg font-black text-emerald-600 dark:text-emerald-400 transition-all leading-none" id="stat-active-users"&gt;0&lt;/div&gt;`  
  
            `&lt;span class="text-[9px] text-emerald-500 dark:text-emerald-400 flex items-center gap-1 font-medium whitespace-nowrap leading-none mb-0.5"&gt;`  
  
                `&lt;span class="w-1.5 h-1.5 bg-emerald-500 rounded-full animate-pulse"&gt;&lt;/span&gt;`  
  
                `متصل در این لحظه`  
  
            `&lt;/span&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
    `&lt;div id="card-cf-requests" class="bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md p-2.5 shadow-sm flex flex-col justify-center gap-1 hover:shadow-md hover:border-orange-400 dark:hover:border-orange-500/50 transition duration-300 relative overflow-hidden group min-h-[64px]"&gt;`  
  
        `&lt;div class="absolute -right-4 -bottom-4 w-16 h-16 bg-orange-500/10 rounded-full blur-xl group-hover:scale-150 transition duration-500"&gt;&lt;/div&gt;`  
  
        `&lt;div class="flex items-center justify-between relative z-10"&gt;`  
  
            `&lt;span class="text-[11px] sm:text-xs font-semibold text-gray-500 dark:text-zinc-400 whitespace-nowrap"&gt;ریکوئست‌های روزانه&lt;/span&gt;`  
  
            `&lt;div class="p-1 bg-orange-50 dark:bg-orange-950/30 text-orange-600 dark:text-orange-400 rounded-md flex-shrink-0"&gt;`  
  
                `&lt;svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 15a4 4 0 004 4h9a5 5 0 10-.1-9.999 5.002 5.002 0 10-9.78 2.096A4.001 4.001 0 003 15z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;div class="relative z-10 min-w-0 flex-1 w-full mt-0.5"&gt;`  
  
            `&lt;div class="flex items-end justify-between w-full mb-1.5"&gt;`  
  
                `&lt;div class="flex items-baseline gap-1"&gt;`  
  
                    `&lt;span class="text-lg font-black text-orange-600 dark:text-orange-400 transition-all leading-none" id="stat-cf-requests"&gt;0&lt;/span&gt;`  
  
                    `&lt;span class="text-[9px] font-bold text-gray-400 mr-0.5 leading-none"&gt;/ 100k&lt;/span&gt;`  
  
                    `&lt;button id="cf-warning-btn" onclick="openUsageWarning()" class="hidden flex items-center justify-center w-3 h-3 bg-red-100 dark:bg-red-900/40 text-red-600 dark:text-red-400 rounded-full font-bold text-[9px] animate-bounce shadow-sm border border-red-300 dark:border-red-700 mr-1 leading-none"&gt;!&lt;/button&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;span class="text-[9px] text-orange-500 dark:text-orange-400 flex items-center gap-1 font-medium whitespace-nowrap leading-none"&gt;`  
  
                    `&lt;span&gt;Total: &lt;span id="stat-cf-total"&gt;0&lt;/span&gt;&lt;/span&gt;`  
  
                `&lt;/span&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;div class="w-full bg-gray-100 dark:bg-zinc-800 rounded-full h-1"&gt;`  
  
                `&lt;div id="stat-cf-progress" class="bg-orange-500 h-1 rounded-full transition-all duration-500" style="width: 0%"&gt;&lt;/div&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
    `&lt;div class="bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md p-2.5 shadow-sm flex flex-col justify-center gap-1 hover:shadow-md hover:border-blue-400 dark:hover:border-blue-500/50 transition duration-300 relative overflow-hidden group min-h-[64px]"&gt;`  
  
        `&lt;div class="absolute -right-4 -bottom-4 w-16 h-16 bg-blue-500/10 rounded-full blur-xl group-hover:scale-150 transition duration-500"&gt;&lt;/div&gt;`  
  
        `&lt;div class="flex items-center justify-between relative z-10"&gt;`  
  
            `&lt;span class="text-[11px] sm:text-xs font-semibold text-gray-500 dark:text-zinc-400 whitespace-nowrap"&gt;ترافیک مصرفی سرور&lt;/span&gt;`  
  
            `&lt;div class="p-1 bg-blue-50 dark:bg-blue-950/30 text-blue-600 dark:text-blue-400 rounded-md flex-shrink-0"&gt;`  
  
                `&lt;svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;div class="flex items-end justify-between relative z-10 w-full mt-0.5"&gt;`  
  
            `&lt;div class="text-lg font-black text-blue-600 dark:text-blue-400 transition-all whitespace-nowrap leading-none" id="stat-total-usage"&gt;0 GB&lt;/div&gt;`  
  
            `&lt;span class="text-[9px] text-blue-500 dark:text-blue-400 flex items-center gap-0.5 font-medium whitespace-nowrap leading-none mb-0.5"&gt;`  
  
                `&lt;svg class="w-2.5 h-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M9 19l3 3m0 0l3-3m-3 3V10"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                `مجموع`  
  
            `&lt;/span&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;/div&gt;`  
  
        `&lt;div id="loading-state" class="text-center py-12"&gt;`  
  
            `&lt;span class="text-gray-500 dark:text-gray-400"&gt;در حال بارگذاری کاربران...&lt;/span&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;div class="mb-5 flex flex-col md:flex-row gap-2 justify-between items-center bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md p-2 shadow-sm"&gt;`  
  
            `&lt;div class="relative w-full md:w-80"&gt;`  
  
                `&lt;input type="text" id="search-input" oninput="filterAndRenderUsers()" placeholder="جستجوی نام کاربری یا UUID..." class="w-full pl-3 pr-8 py-1.5 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-xs"&gt;`  
  
                `&lt;div class="absolute inset-y-0 right-0 flex items-center pr-2.5 pointer-events-none text-gray-400"&gt;`  
  
                    `&lt;svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                `&lt;/div&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;div class="flex items-center gap-2 w-full md:w-auto"&gt;`  
  
                `&lt;select id="filter-status" onchange="filterAndRenderUsers()" class="flex-1 min-w-0 px-2 py-1.5 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md text-xs focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-700 dark:text-zinc-300 cursor-pointer truncate"&gt;`  
  
                    `&lt;option value="all"&gt;🔍 همه&lt;/option&gt;`  
  
					`&lt;option value="active"&gt;✅ فعال&lt;/option&gt;`  
  
                    `&lt;option value="inactive"&gt;❌ غیرفعال&lt;/option&gt;`  
  
                    `&lt;option value="online"&gt;⚡ آنلاین&lt;/option&gt;`  
  
                    `&lt;option value="offline"&gt;💤 آفلاین&lt;/option&gt;`  
  
                    `&lt;option value="expired"&gt;⏳ منقضی&lt;/option&gt;`  
  
                `&lt;/select&gt;`  
  
                `&lt;select id="sort-users" onchange="filterAndRenderUsers()" class="flex-1 min-w-0 px-2 py-1.5 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md text-xs focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-700 dark:text-zinc-300 cursor-pointer truncate"&gt;`  
  
                    `&lt;option value="newest"&gt;📅 جدیدترین&lt;/option&gt;`  
  
                    `&lt;option value="name"&gt;🔤 نام کاربری (الفبا)&lt;/option&gt;`  
  
                    `&lt;option value="usage-desc"&gt;📊 بیشترین مصرف&lt;/option&gt;`  
  
                    `&lt;option value="usage-asc"&gt;📈 کمترین مصرف&lt;/option&gt;`  
  
                    `&lt;option value="expiry-asc"&gt;⏳ کمترین زمان باقی‌مانده&lt;/option&gt;`  
  
                `&lt;/select&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
		`&lt;div class="flex items-center justify-between mb-4"&gt;`  
  
			`&lt;h2 class="text-lg font-bold text-gray-800 dark:text-zinc-200"&gt;لیست کاربران&lt;/h2&gt;`  
  
			`&lt;button onclick="openCreateModal()" class="p-2 rounded-md bg-blue-50 dark:bg-blue-900/30 border-2 border-blue-200 dark:border-blue-800/50 hover:bg-blue-100 dark:hover:bg-blue-900/50 transition-all duration-300 text-blue-600 dark:text-blue-400 shadow-sm hover:shadow hover:scale-110"&gt;`  
  
    			`&lt;svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M12 4v16m8-8H4"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
			`&lt;/button&gt;`  
  
		`&lt;/div&gt;`  
  
        `&lt;div id="users-table-container" class="hidden overflow-x-auto border border-gray-200 dark:border-amoled-border rounded-md bg-white dark:bg-amoled-card"&gt;`  
  
            `&lt;table class="w-full text-right border-collapse"&gt;`  
  
                `&lt;thead&gt;`  
  
                    `&lt;tr class="bg-gray-100 dark:bg-zinc-900/50 border-b border-gray-200 dark:border-amoled-border text-xs text-gray-500 dark:text-gray-400 text-center"&gt;`  
  
                        `&lt;th class="p-2 w-10 text-center"&gt;&lt;input type="checkbox" id="select-all-users" onchange="toggleSelectAllUsers(this)" class="w-5 h-5 rounded-md border-2 border-gray-300 dark:border-zinc-700 text-blue-600 bg-white dark:bg-zinc-800 checked:bg-blue-600 checked:border-blue-600 focus:ring-blue-500/50 focus:ring-offset-0 transition-all duration-200 cursor-pointer hover:scale-105 active:scale-95"&gt;&lt;/th&gt;`  
  
                        `&lt;th class="p-2 border-r border-gray-200 dark:border-zinc-800"&gt;وضعیت&lt;/th&gt;`  
  
                        `&lt;th class="p-2 border-r border-gray-200 dark:border-zinc-800"&gt;عملیات&lt;/th&gt;`  
  
                        `&lt;th class="p-2 border-r border-gray-200 dark:border-zinc-800"&gt;لینک ساب&lt;/th&gt;`  
  
                        `&lt;th class="p-2 border-r border-gray-200 dark:border-zinc-800"&gt;پورت&lt;/th&gt;`  
  
                        `&lt;th class="p-2 border-r border-gray-200 dark:border-zinc-800"&gt;حجم&lt;/th&gt;`  
  
                        `&lt;th class="p-2 border-r border-gray-200 dark:border-zinc-800"&gt;ریکوئست&lt;/th&gt;`  
  
                        `&lt;th class="p-2 border-r border-gray-200 dark:border-zinc-800"&gt;زمان&lt;/th&gt;`  
  
                        `&lt;th class="p-2 border-r border-gray-200 dark:border-zinc-800"&gt;کاربران آنلاین&lt;/th&gt;`  
  
                    `&lt;/tr&gt;`  
  
                `&lt;/thead&gt;`  
  
                `&lt;tbody id="users-tbody" class="divide-y divide-gray-150 dark:divide-amoled-border text-sm"&gt;&lt;/tbody&gt;`  
  
            `&lt;/table&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;div id="empty-state" class="hidden p-8 border-2 border-dashed border-red-500/60 dark:border-red-500/50 bg-red-50 dark:bg-red-900/10 rounded-md text-center animate-pulse shadow-sm"&gt;`  
  
            `&lt;p class="text-red-600 dark:text-red-400 font-bold text-lg"&gt;کاربری وجود ندارد. برای ساخت اولین کاربر روی دکمه « + » کلیک کنید.&lt;/p&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/main&gt;`  
  
`&lt;div id="usage-warning-modal" class="fixed inset-0 z-[80] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out"&gt;`  
  
    `&lt;div class="w-full max-w-md bg-white dark:bg-amoled-card border border-orange-500/50 rounded-md shadow-2xl overflow-hidden p-6 text-center transition-all transform duration-300 opacity-0 scale-95 ease-out"&gt;`  
  
        `&lt;div class="inline-flex items-center justify-center w-16 h-16 rounded-full bg-orange-100 dark:bg-orange-900/30 text-orange-500 mb-4 shadow-inner"&gt;`  
  
            `&lt;svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;h3 class="font-black text-xl text-gray-900 dark:text-white mb-2"&gt;هشدار محدودیت درخواست روزانه&lt;/h3&gt;`  
  
        `&lt;p class="text-sm text-gray-600 dark:text-gray-400 mb-6 leading-relaxed font-medium"&gt;`  
  
            `درخواست‌های روزانه کلودفلر شما از ۹۰,۰۰۰ عبور کرده است. در صورت عبور از محدودیت رایگان ۱۰۰,۰۰۰ درخواست، دسترسی به پـنـل و اتصالات تا ساعت ۳:۳۰ بامداد (به وقت ایران) قطع خواهد شد.`  
  
        `&lt;/p&gt;`  
  
        `&lt;button onclick="closeUsageWarning()" class="w-full py-3.5 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-black rounded-md text-sm transition duration-300 shadow-lg"&gt;`  
  
            `متوجه شدم`  
  
        `&lt;/button&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;/div&gt;`  
  
`&lt;div id="free-panel-warning-modal" class="fixed inset-0 z-[85] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out"&gt;`  
  
    `&lt;div class="w-full max-w-md bg-white dark:bg-amoled-card border border-rose-500/50 rounded-md shadow-2xl overflow-hidden p-6 text-center transition-all transform duration-300 opacity-0 scale-95 ease-out"&gt;`  
  
        `&lt;div class="inline-flex items-center justify-center w-16 h-16 rounded-full bg-rose-100 dark:bg-rose-900/30 text-rose-500 mb-4 shadow-inner"&gt;`  
  
            `&lt;svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;h3 class="font-black text-xl text-gray-900 dark:text-white mb-2"&gt;پیام همگانی&lt;/h3&gt;`  
  
        `&lt;p class="text-sm text-gray-600 dark:text-gray-400 mb-6 leading-relaxed font-medium"&gt;`  
  
            `این پـنـل کاملاً &lt;span class="text-rose-500 font-bold"&gt;رایگان&lt;/span&gt; است. هرگونه فروش پـنـل یا کـانفـیگ‌های آن مصداق کلاه‌برداری و رفتاری دور از انسانیت و شرافت است. لطفاً از این ابزار فقط به صورت شخصی و رایگان استفاده کنید.`  
  
        `&lt;/p&gt;`  
  
        `&lt;button onclick="closeFreePanelWarning()" class="w-full py-3.5 bg-transparent border-2 border-green-800 text-green-900 hover:bg-green-800 hover:text-white dark:border-green-800 dark:text-green-700 dark:hover:bg-green-900 dark:hover:text-white font-black rounded-md text-sm transition duration-300 shadow-lg"&gt;`  
  
            `تأیید و موافقت`  
  
        `&lt;/button&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;/div&gt;`  
  
`&lt;div id="global-message-modal" class="fixed inset-0 z-[86] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out"&gt;`  
  
    `&lt;div class="w-full max-w-md bg-white dark:bg-amoled-card border border-blue-500/50 rounded-md shadow-2xl overflow-hidden p-6 text-center transition-all transform duration-300 opacity-0 scale-95 ease-out"&gt;`  
  
        `&lt;div class="inline-flex items-center justify-center w-16 h-16 rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-500 mb-4 shadow-inner"&gt;`  
  
            `&lt;svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;h3 class="font-black text-xl text-gray-900 dark:text-white mb-4"&gt;پیام همگانی&lt;/h3&gt;`  
  
        `&lt;div id="global-message-content" class="mb-6 w-full text-center"&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;button id="global-message-close-btn" class="w-full py-3.5 bg-transparent border-2 border-blue-600 text-blue-700 hover:bg-blue-900/20 hover:text-blue-800 dark:border-blue-500 dark:text-blue-500 dark:hover:bg-blue-900/40 dark:hover:text-blue-400 font-black rounded-md text-sm transition duration-300 shadow-lg"&gt;`  
  
            `متوجه شدم`  
  
        `&lt;/button&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;/div&gt;`  
  
    `&lt;div id="user-modal" class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 opacity-0 pointer-events-none transition-opacity duration-200 ease-out"&gt;`  
  
        `&lt;div id="user-modal-card" class="w-full max-w-xl lg:max-w-[1200px] bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md shadow-xl overflow-hidden transition-[opacity,transform] duration-200 opacity-0 scale-95 ease-out flex flex-col max-h-[90vh] transform-gpu" style="will-change: transform, opacity;"&gt;`  
  
            `&lt;div class="px-6 py-4 border-b border-gray-150 dark:border-amoled-border flex justify-between items-center bg-gray-50/50 dark:bg-amoled-bg"&gt;`  
  
                `&lt;div class="flex items-center gap-2"&gt;`  
  
                    `&lt;div class="w-2.5 h-2.5 rounded-full bg-blue-500"&gt;&lt;/div&gt;`  
  
                    `&lt;h3 id="modal-title" class="font-bold text-gray-900 dark:text-zinc-100 text-base"&gt;ایجاد کاربر جدید&lt;/h3&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;button onclick="toggleModal(false)" class="p-1.5 rounded-md bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-900/50 text-red-600 dark:text-red-500 hover:bg-red-100 dark:hover:bg-red-900/50 transition-all duration-200 shadow-sm"&gt;`  
  
                    `&lt;svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                `&lt;/button&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;form id="create-user-form" class="p-4 flex flex-col overflow-y-auto flex-1 overscroll-contain" style="-webkit-overflow-scrolling: touch; transform: translate3d(0,0,0); will-change: scroll-position, transform;" onsubmit="handleFormSubmit(event)"&gt;`  
  
				`&lt;input type="hidden" id="hidden-auto-rotate" value="0"&gt;`  
  
				`&lt;input type="hidden" id="hidden-rotate-time" value=""&gt;`  
  
				`&lt;input type="hidden" id="hidden-ip-operator" value="all"&gt;`  
  
				`&lt;input type="hidden" id="hidden-ip-count" value="20"&gt;`  
  
                `&lt;div class="grid grid-cols-1 lg:grid-cols-3 gap-4 lg:gap-6 flex-1"&gt;`  
  
                    `&lt;div class="flex flex-col gap-3"&gt;`  
  
                        `&lt;div class="space-y-2.5"&gt;`  
  
                            `&lt;div&gt;`  
  
                                `&lt;label class="block text-[11px] font-bold text-gray-500 dark:text-zinc-400 mb-1 uppercase tracking-wider"&gt;نام کاربری&lt;/label&gt;`  
  
                                `&lt;div class="relative"&gt;`  
  
                                    `&lt;span class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none text-gray-400"&gt;`  
  
                                        `&lt;svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                                    `&lt;/span&gt;`  
  
                                    `&lt;input type="text" id="input-name" oninput="this.value = this.value.replace(/[^a-zA-Z0-9_-]/g, '')" placeholder="Z_E_U_S" maxlength="32" class="w-full pl-3 pr-9 py-1.5 bg-gray-50 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition" required&gt;`  
  
                                `&lt;/div&gt;`  
  
                            `&lt;/div&gt;`  
  
                            `&lt;div class="grid grid-cols-2 gap-2.5"&gt;`  
  
                                `&lt;div&gt;`  
  
                                    `&lt;label class="block text-[10px] font-bold text-gray-500 dark:text-zinc-400 mb-1 uppercase tracking-wider"&gt;حجم (GB)&lt;/label&gt;`  
  
                                    `&lt;div class="relative"&gt;`  
  
                                        `&lt;span class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none text-gray-400"&gt;`  
  
                                            `&lt;svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                                        `&lt;/span&gt;`  
  
                                        `&lt;input type="number" id="input-limit" min="0" step="any" placeholder="نامحدود" class="w-full pl-3 pr-9 py-1.5 bg-gray-50 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition"&gt;`  
  
                                    `&lt;/div&gt;`  
  
                                `&lt;/div&gt;`  
  
                                `&lt;div&gt;`  
  
                                    `&lt;label class="block text-[10px] font-bold text-gray-500 dark:text-zinc-400 mb-1 uppercase tracking-wider"&gt;زمان (روز)&lt;/label&gt;`  
  
                                    `&lt;div class="relative"&gt;`  
  
                                        `&lt;span class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none text-gray-400"&gt;`  
  
                                            `&lt;svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                                        `&lt;/span&gt;`  
  
                                        `&lt;input type="number" id="input-expiry" min="0" placeholder="نامحدود" class="w-full pl-3 pr-9 py-1.5 bg-gray-50 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition"&gt;`  
  
                                    `&lt;/div&gt;`  
  
                                `&lt;/div&gt;`  
  
                                `&lt;div&gt;`  
  
                                    `&lt;label class="block text-[10px] font-bold text-gray-500 dark:text-zinc-400 mb-1 uppercase tracking-wider"&gt;سقف ریکوئست&lt;/label&gt;`  
  
                                    `&lt;div class="relative"&gt;`  
  
                                        `&lt;span class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none text-gray-400"&gt;`  
  
                                            `&lt;svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                                        `&lt;/span&gt;`  
  
                                        `&lt;input type="number" id="input-req-limit" min="0" placeholder="نامحدود" class="w-full pl-3 pr-9 py-1.5 bg-gray-50 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition"&gt;`  
  
                                    `&lt;/div&gt;`  
  
                                `&lt;/div&gt;`  
  
                                `&lt;div&gt;`  
  
                                    `&lt;label class="block text-[10px] font-bold text-gray-500 dark:text-zinc-400 mb-1 uppercase tracking-wider"&gt;محدودیت کاربر&lt;/label&gt;`  
  
                                    `&lt;div class="relative"&gt;`  
  
                                        `&lt;span class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none text-gray-400"&gt;`  
  
                                            `&lt;svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                                        `&lt;/span&gt;`  
  
                                        `&lt;input type="number" id="input-ip-limit" min="0" placeholder="نامحدود" class="w-full pl-3 pr-9 py-1.5 bg-gray-50 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition"&gt;`  
  
                                    `&lt;/div&gt;`  
  
                                `&lt;/div&gt;`  
  
                            `&lt;/div&gt;`  
  
                        `&lt;/div&gt;`  
  
                        `&lt;div class="flex flex-col gap-3 border border-gray-100 dark:border-amoled-border p-3 rounded-md bg-gray-50 dark:bg-amoled-input"&gt;`  
  
                            `&lt;div class="flex items-center justify-between"&gt;`  
  
                                `&lt;div class="flex items-center gap-2"&gt;`  
  
                                    `&lt;svg class="w-4 h-4 text-gray-500 dark:text-zinc-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                                    `&lt;span class="text-[11px] font-bold text-gray-500 dark:text-zinc-400 uppercase tracking-wider"&gt;تمدید خودکار (۳:۳۰ بامداد)&lt;/span&gt;`  
  
                                `&lt;/div&gt;`  
  
                                `&lt;label class="relative inline-flex items-center cursor-pointer select-none"&gt;`  
  
                                    `&lt;input type="checkbox" id="input-auto-reset-toggle" onchange="toggleAutoResetInputs(this.checked)" class="sr-only peer"&gt;`  
  
                                    `&lt;div class="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:bg-green-600 transition-colors after:content-[''] after:absolute after:top-[2px] after:right-[2px] after:bg-white after:rounded-full after:h-4 after:w-4 after:transition-transform peer-checked:after:-translate-x-[18px]"&gt;&lt;/div&gt;`  
  
                                `&lt;/label&gt;`  
  
                            `&lt;/div&gt;`  
  
                            `&lt;div id="auto-reset-inputs-container" class="grid grid-cols-2 gap-2 transition-all duration-300 pt-2 border-t border-gray-100 dark:border-amoled-border opacity-50 pointer-events-none"&gt;`  
  
                                `&lt;div&gt;`  
  
                                    `&lt;label class="block text-[10px] font-bold text-gray-500 dark:text-zinc-400 mb-1 uppercase tracking-wider"&gt;زمان تمدید حجم (روز)&lt;/label&gt;`  
  
                                    `&lt;input type="number" id="input-auto-reset-vol" min="1" placeholder="خالی = بدون تمدید" class="w-full px-2 py-1.5 bg-gray-50 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-mono text-center text-gray-800 dark:text-zinc-100 transition" dir="ltr" disabled&gt;`  
  
                                `&lt;/div&gt;`  
  
                                `&lt;div&gt;`  
  
                                    `&lt;label class="block text-[10px] font-bold text-gray-500 dark:text-zinc-400 mb-1 uppercase tracking-wider"&gt;زمان تمدید ریکوئست (روز)&lt;/label&gt;`  
  
                                    `&lt;input type="number" id="input-auto-reset-req" min="1" placeholder="خالی = بدون تمدید" class="w-full px-2 py-1.5 bg-gray-50 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-mono text-center text-gray-800 dark:text-zinc-100 transition" dir="ltr" disabled&gt;`  
  
                                `&lt;/div&gt;`  
  
                            `&lt;/div&gt;`  
  
                        `&lt;/div&gt;`  
  
                        `&lt;div class="grid grid-cols-2 gap-3"&gt;`  
  
                            `&lt;div class="p-3 bg-gray-50 dark:bg-amoled-input border border-gray-200/60 dark:border-amoled-border rounded-md shadow-sm"&gt;`  
  
                                `&lt;div class="flex items-center justify-between mb-2"&gt;`  
  
                                    `&lt;span class="text-[10px] font-bold text-gray-500 dark:text-zinc-400 uppercase tracking-wider"&gt;Fragment&lt;/span&gt;`  
  
                                    `&lt;label class="relative inline-flex items-center cursor-pointer select-none"&gt;`  
  
                                        `&lt;input type="checkbox" id="input-frag-toggle" onchange="toggleFragInputs(this.checked)" class="sr-only peer" checked&gt;`  
  
                                        `&lt;div class="w-9 h-5 bg-gray-200 rounded-full peer dark:bg-zinc-700 peer-checked:bg-green-600 transition-colors after:content-[''] after:absolute after:top-[2px] after:right-[2px] after:bg-white after:rounded-full after:h-4 after:w-4 after:transition-transform peer-checked:after:-translate-x-[18px]"&gt;&lt;/div&gt;`  
  
                                    `&lt;/label&gt;`  
  
                                `&lt;/div&gt;`  
  
                                `&lt;div id="frag-inputs-container" class="grid grid-cols-2 gap-1.5 transition-all duration-300"&gt;`  
  
                                    `&lt;input type="text" id="input-frag-len" placeholder="Len" value="200-3000" dir="ltr" class="w-full px-1.5 py-1 bg-white dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500 text-[10px] font-mono text-center text-gray-800 dark:text-zinc-100"&gt;`  
  
                                    `&lt;input type="text" id="input-frag-int" placeholder="Int" value="1-2" dir="ltr" class="w-full px-1.5 py-1 bg-white dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500 text-[10px] font-mono text-center text-gray-800 dark:text-zinc-100"&gt;`  
  
                                `&lt;/div&gt;`  
  
                            `&lt;/div&gt;`  
  
                            `&lt;div class="p-3 bg-gray-50 dark:bg-amoled-input border border-gray-200/60 dark:border-amoled-border rounded-md shadow-sm"&gt;`  
  
                                `&lt;label class="block text-[10px] font-bold text-gray-500 dark:text-zinc-400 mb-1.5 uppercase tracking-wider"&gt;Fingerprint&lt;/label&gt;`  
  
                                `&lt;div class="relative"&gt;`  
  
                                    `&lt;select id="fingerprint-select" class="w-full px-2 py-1.5 bg-white dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500 text-[10px] font-semibold text-gray-700 dark:text-zinc-300 cursor-pointer appearance-none"&gt;`  
  
                                        `&lt;option value="chrome"&gt;🌐 Chrome&lt;/option&gt;`  
  
                                        `&lt;option value="firefox"&gt;🦊 Firefox&lt;/option&gt;`  
  
                                        `&lt;option value="safari"&gt;🧭 Safari&lt;/option&gt;`  
  
                                        `&lt;option value="ios" selected&gt;📱 iOS (پیشنهادی)&lt;/option&gt;`  
  
                                        `&lt;option value="android"&gt;🤖 Android&lt;/option&gt;`  
  
                                        `&lt;option value="edge"&gt;🌀 Edge&lt;/option&gt;`  
  
                                        `&lt;option value="360"&gt;🔒 360 Browser&lt;/option&gt;`  
  
                                        `&lt;option value="qq"&gt;💬 QQ Browser&lt;/option&gt;`  
  
                                        `&lt;option value="random"&gt;🎲 Random&lt;/option&gt;`  
  
                                        `&lt;option value="randomized"&gt;🎭 Dynamic&lt;/option&gt;`  
  
                                    `&lt;/select&gt;`  
  
                                    `&lt;div class="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-2 text-gray-500"&gt;`  
  
                                        `&lt;svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                                    `&lt;/div&gt;`  
  
                                `&lt;/div&gt;`  
  
                            `&lt;/div&gt;`  
  
                        `&lt;/div&gt;`  
  
                        `&lt;div class="grid grid-cols-2 gap-2"&gt;`  
  
                            `&lt;div class="flex items-center justify-between bg-gray-50 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md p-1.5 shadow-sm"&gt;`  
  
                                `&lt;span class="text-[10px] sm:text-xs font-semibold text-gray-700 dark:text-zinc-300 whitespace-nowrap pl-1"&gt;NSFW BLOCKER&lt;/span&gt;`  
  
                                `&lt;label class="relative inline-flex items-center cursor-pointer scale-[0.65] sm:scale-75 origin-left"&gt;`  
  
                                    `&lt;input type="checkbox" id="input-block-porn" class="sr-only peer"&gt;`  
  
                                    `&lt;div class="w-11 h-6 bg-gray-300 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:right-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-green-700"&gt;&lt;/div&gt;`  
  
                                `&lt;/label&gt;`  
  
                            `&lt;/div&gt;`  
  
                            `&lt;div class="flex items-center justify-between bg-gray-50 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md p-1.5 shadow-sm"&gt;`  
  
                                `&lt;span class="text-[10px] sm:text-xs font-semibold text-gray-700 dark:text-zinc-300 whitespace-nowrap pl-1"&gt;ADS BLOCKER&lt;/span&gt;`  
  
                                `&lt;label class="relative inline-flex items-center cursor-pointer scale-[0.65] sm:scale-75 origin-left"&gt;`  
  
                                    `&lt;input type="checkbox" id="input-block-ads" class="sr-only peer"&gt;`  
  
                                    `&lt;div class="w-11 h-6 bg-gray-300 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:right-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-green-700"&gt;&lt;/div&gt;`  
  
                                `&lt;/label&gt;`  
  
                            `&lt;/div&gt;`  
  
                        `&lt;/div&gt;`  
  
                    `&lt;/div&gt;`  
  
                    `&lt;div class="flex flex-col pt-4 lg:pt-0 border-t-2 lg:border-t-0 lg:border-x-2 border-gray-300 dark:border-amoled-border lg:px-4 h-full"&gt;`  
  
                        `&lt;label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 mb-3 uppercase tracking-wider"&gt;پورت‌های اتصال&lt;/label&gt;`  
  
                        `&lt;div class="grid grid-cols-2 gap-2 md:gap-4"&gt;`  
  
                            `&lt;div class="p-3 bg-gray-50 dark:bg-amoled-input border border-gray-200/60 dark:border-amoled-border rounded-md shadow-sm flex flex-col"&gt;`  
  
                                `&lt;div class="flex items-center gap-1.5 mb-2"&gt;`  
  
                                    `&lt;span class="flex h-2 w-2 rounded-full bg-blue-500 shadow-sm"&gt;&lt;/span&gt;`  
  
                                    `&lt;span class="text-[11px] font-bold text-blue-600 dark:text-blue-400"&gt;🔒TLS PORT&lt;/span&gt;`  
  
                                `&lt;/div&gt;`  
  
                                `&lt;div class="grid grid-cols-3 gap-1.5 flex-1 content-start" id="tls-ports-list"&gt;&lt;/div&gt;`  
  
                            `&lt;/div&gt;`  
  
                            `&lt;div class="p-3 bg-gray-50 dark:bg-amoled-input border border-gray-200/60 dark:border-amoled-border rounded-md shadow-sm flex flex-col"&gt;`  
  
                                `&lt;div class="flex items-center gap-1.5 mb-2"&gt;`  
  
                                    `&lt;span class="flex h-2 w-2 rounded-full bg-amber-500 shadow-sm"&gt;&lt;/span&gt;`  
  
                                    `&lt;span class="text-[11px] font-bold text-amber-600 dark:text-amber-400"&gt;🔓Non-TLS PORT&lt;/span&gt;`  
  
                                `&lt;/div&gt;`  
  
                                `&lt;div class="grid grid-cols-3 gap-1.5 flex-1 content-start" id="nontls-ports-list"&gt;&lt;/div&gt;`  
  
                            `&lt;/div&gt;`  
  
                        `&lt;/div&gt;`  
  
                        `&lt;div class="mt-4 p-3 bg-gray-50 dark:bg-amoled-input border border-gray-200/60 dark:border-amoled-border rounded-md shadow-sm"&gt;`  
  
                            `&lt;div class="flex items-center gap-1.5 mb-2"&gt;`  
  
                                `&lt;span class="flex h-2 w-2 rounded-full bg-green-600 shadow-sm"&gt;&lt;/span&gt;`  
  
                                `&lt;span class="text-[11px] font-bold text-green-700 dark:text-green-500"&gt;⚙️ پورت‌های دلخواه (با فاصله جدا کنید)&lt;/span&gt;`  
  
                            `&lt;/div&gt;`  
  
                            `&lt;input type="text" id="input-custom-ports" placeholder="8080 2096 5000" dir="ltr" class="w-full px-2 py-2 bg-white dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-mono text-left text-gray-800 dark:text-zinc-100 transition"&gt;`  
  
                        `&lt;/div&gt;`  
  
                        `&lt;div class="flex flex-col flex-1 mt-4 pt-4 border-t-2 border-gray-300 dark:border-amoled-border"&gt;`  
  
                            `&lt;div class="flex items-center justify-between mb-2"&gt;`  
  
                                `&lt;label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 uppercase tracking-wider"&gt;آیپی تمیز (توصیه میشود)&lt;/label&gt;`  
  
                                `&lt;button type="button" onclick="openIpSelectorModal()" class="px-2.5 py-1 bg-amber-50 dark:bg-amber-950/60 text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/70 border border-amber-400 dark:border-amber-600 rounded-md text-xs font-bold transition-all"&gt;مخزن آیپی تمیز&lt;/button&gt;`  
  
                            `&lt;/div&gt;`  
  
                            `&lt;textarea id="input-ips" placeholder="104.16.0.1" class="w-full h-full min-h-[80px] flex-1 px-3 py-2.5 bg-gray-50 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-mono text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition resize-none"&gt;&lt;/textarea&gt;`  
  
                        `&lt;/div&gt;`  
  
                    `&lt;/div&gt;`  
  
                    `&lt;div class="flex flex-col gap-2 pt-4 lg:pt-0 border-t-2 lg:border-t-0 border-gray-300 dark:border-amoled-border justify-between"&gt;`  
  
                        `&lt;div class="flex flex-col flex-1"&gt;`  
  
                            `&lt;div class="flex items-center gap-2 mb-3"&gt;`  
  
                                `&lt;label class="relative inline-flex items-center cursor-pointer select-none flex-shrink-0"&gt;`  
  
                                    `&lt;input type="checkbox" id="user-proxy-mode-toggle" onchange="toggleUserProxyMode(this.checked)" class="sr-only peer"&gt;`  
  
                                    `&lt;div class="w-7 h-4 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:right-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-3 after:w-3 after:transition-all dark:border-gray-600 peer-checked:bg-green-700"&gt;&lt;/div&gt;`  
  
                                `&lt;/label&gt;`  
  
                                `&lt;label class="block text-xs sm:text-sm font-bold text-gray-700 dark:text-zinc-300 cursor-pointer truncate" onclick="document.getElementById('user-proxy-mode-toggle').click()"&gt;ثابت کردن کشور و آیپی با تنظیم پـروکـسـی &lt;/label&gt;`  
  
                            `&lt;/div&gt;`  
  
                            `&lt;div class="grid grid-cols-2 gap-2 mb-2 w-full"&gt;`  
  
                                `&lt;button type="button" onclick="toggleDonateModal(true)" class="text-[11px] bg-emerald-50 dark:bg-emerald-900/30 text-emerald-600 dark:text-emerald-400 px-2 py-2 rounded border border-emerald-200 dark:border-emerald-800 hover:bg-emerald-100 dark:hover:bg-emerald-900/50 transition font-black shadow-sm text-center whitespace-nowrap"&gt;اهدای پـروکـسـی شخصی ❤️&lt;/button&gt;`  
  
                                `&lt;a href="[https://github.com/mkvaslim44/Ma_ke_vaslim-Panel#%EF%B8%8F-build-your-own-socks5-proxy-zeus-relay](https://github.com/mkvaslim44/Ma_ke_vaslim-Panel#%EF%B8%8F-build-your-own-socks5-proxy-zeus-relay)" target="_blank" class="text-[11px] bg-blue-50 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 px-2 py-2 rounded border border-blue-200 dark:border-blue-800 hover:bg-blue-100 dark:hover:bg-blue-900/50 transition font-black shadow-sm text-center whitespace-nowrap"&gt;ساخت پـروکـسـی شخصی&lt;/a&gt;`  
  
                            `&lt;/div&gt;`  
  
                            `&lt;div class="mb-2 p-2 border-2 border-dashed border-red-500 bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-400 rounded-md text-[11px] font-bold leading-relaxed text-center w-full shadow-[0_0_15px_rgba(239,68,68,0.6)]" style="animation: pulse 1s cubic-bezier(0.4, 0, 0.6, 1) infinite, alertShake 2s infinite;"&gt;`  
  
                                `&lt;style&gt;@keyframes alertShake { 0%, 100% {transform: translateX(0);} 2%, 6%, 10% {transform: translateX(-3px);} 4%, 8%, 12% {transform: translateX(3px);} 14% {transform: translateX(0);} }&lt;/style&gt;`  
  
                                `سایت‌هایی مثل &lt;span class="text-emerald-600 dark:text-emerald-400 font-black"&gt;ChatGPT&lt;/span&gt; و &lt;span class="text-amber-600 dark:text-amber-400 font-black"&gt;Claude&lt;/span&gt; پشت کلودفلر هستند؛ برای باز کردن این سایت‌ها حتماً باید &lt;span class="text-blue-600 dark:text-blue-400 font-black"&gt;پـروکـسـی&lt;/span&gt; تنظیم کنید.`  
  
                            `&lt;/div&gt;`  
  
                            `&lt;div class="relative transition-opacity duration-300 opacity-50 pointer-events-none flex-1 flex flex-col justify-start" id="user-socks5-container"&gt;`  
  
                                `&lt;input type="text" id="user-socks5-input" placeholder="socks5:// یا http:// یا (user:pass@ip:port)" dir="ltr" class="w-full px-3 py-2.5 bg-gray-50 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border rounded-md text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-800 dark:text-zinc-100 transition" disabled&gt;`  
  
                                `&lt;div class="w-full text-center"&gt;`  
  
                                    `&lt;span id="test-user-proxy-result" class="inline-block mt-2 text-[11px] font-bold transition-colors break-words leading-relaxed empty:hidden"&gt;&lt;/span&gt;`  
  
                                `&lt;/div&gt;`  
  
                                `&lt;div class="mt-2 flex items-center justify-between w-full gap-2"&gt;`  
  
                                    `&lt;button type="button" onclick="testUserSocksProxy()" id="test-user-proxy-btn" class="flex-1 text-center text-[11px] bg-sky-50 dark:bg-sky-900/30 text-sky-600 dark:text-sky-400 py-1.5 rounded border border-sky-200 dark:border-sky-800 hover:bg-sky-100 dark:hover:bg-sky-900/50 transition font-bold shadow-sm"&gt;تست پـروکـسـی&lt;/button&gt;`  
  
                                    `&lt;button type="button" onclick="openProxySelectorModal()" class="flex-1 text-center text-[11px] bg-amber-50 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400 py-1.5 rounded border border-amber-200 dark:border-amber-800 hover:bg-amber-100 dark:hover:bg-amber-900/50 transition font-bold shadow-sm"&gt;مخزن پـروکـسـی&lt;/button&gt;`  
  
                                `&lt;/div&gt;`  
  
                                `&lt;div class="mt-2 flex items-center justify-between border border-gray-100 dark:border-amoled-border p-3 rounded-md bg-gray-50 dark:bg-amoled-input"&gt;`  
  
                                    `&lt;div class="flex items-center gap-2"&gt;`  
  
                                        `&lt;svg class="w-4 h-4 text-gray-500 dark:text-zinc-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                                        `&lt;span class="text-[11px] font-bold text-gray-500 dark:text-zinc-400 uppercase tracking-wider"&gt;تعویض خودکار پـروکـسـی (پیشنهادی)&lt;/span&gt;`  
  
                                    `&lt;/div&gt;`  
  
                                    `&lt;label class="relative inline-flex items-center cursor-pointer select-none"&gt;`  
  
                                        `&lt;input type="checkbox" id="input-auto-rotate-user-proxy" class="sr-only peer"&gt;`  
  
                                        `&lt;div class="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:bg-green-600 transition-colors after:content-[''] after:absolute after:top-[2px] after:right-[2px] after:bg-white after:rounded-full after:h-4 after:w-4 after:transition-transform peer-checked:after:-translate-x-[18px]"&gt;&lt;/div&gt;`  
  
                                    `&lt;/label&gt;`  
  
                                `&lt;/div&gt;`  
  
                            `&lt;/div&gt;`  
  
                        `&lt;/div&gt;`  
  
                    `&lt;/div&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;div class="pt-4 flex gap-3 mt-4 border-t border-gray-200 dark:border-amoled-border"&gt;`  
  
                    `&lt;button type="button" onclick="toggleModal(false)" class="flex-1 py-3 bg-transparent border-2 border-rose-700 text-rose-700 hover:bg-rose-900/20 hover:text-rose-800 dark:border-rose-700 dark:text-rose-500 dark:hover:bg-rose-900/40 dark:hover:text-rose-400 font-bold rounded-md text-sm transition duration-200 shadow-sm"&gt;انصراف&lt;/button&gt;`  
  
                    `&lt;button type="submit" id="submit-btn" class="flex-1 py-3 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-bold rounded-md text-sm transition duration-200 shadow-md hover:shadow-lg"&gt;ایجاد کاربر&lt;/button&gt;`  
  
                `&lt;/div&gt;`  
  
            `&lt;/form&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;div id="ip-selector-modal" class="fixed inset-0 z-[60] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out"&gt;`  
  
    `&lt;div class="w-full max-w-sm bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md shadow-xl overflow-hidden transition-all transform duration-300 opacity-0 scale-95 ease-out"&gt;`  
  
        `&lt;div class="px-6 py-4 border-b border-gray-150 dark:border-amoled-border flex justify-between items-center bg-gray-50 dark:bg-zinc-900/50"&gt;`  
  
            `&lt;h3 class="font-bold text-gray-900 dark:text-zinc-100 text-sm"&gt;مخزن آیپی تمیز&lt;/h3&gt;`  
  
            `&lt;button type="button" onclick="toggleIpSelectorModal(false)" class="p-1.5 rounded-md bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-900/50 text-red-600 dark:text-red-500 hover:bg-red-100 dark:hover:bg-red-900/50 transition-all duration-200 shadow-sm"&gt;`  
  
                `&lt;svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
            `&lt;/button&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;div class="p-6 space-y-4"&gt;`  
  
            `&lt;div id="ip-loading-state" class="text-center text-sm text-gray-500 dark:text-zinc-400 hidden"&gt;`  
  
                `Loading IPs...`  
  
            `&lt;/div&gt;`  
  
            `&lt;div id="ip-selection-form" class="space-y-4"&gt;`  
  
                `&lt;div&gt;`  
  
                    `&lt;label class="block text-xs font-medium mb-1.5 text-gray-700 dark:text-zinc-300"&gt;اوپراتور&lt;/label&gt;`  
  
                    `&lt;select id="ip-operator-select" class="w-full px-3 py-2.5 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md text-xs focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-700 dark:text-zinc-300 cursor-pointer"&gt;`  
  
                        `&lt;option value="all"&gt;همه (توصیه شده)&lt;/option&gt;`  
  
                    `&lt;/select&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;div&gt;`  
  
                    `&lt;label class="block text-xs font-medium mb-1.5 text-gray-700 dark:text-zinc-300"&gt;تعداد&lt;/label&gt;`  
  
                    `&lt;input type="number" id="ip-count-input" min="1" value="20" dir="ltr" class="w-full px-3 py-2.5 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-xs font-mono text-center"&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;div class="flex flex-col gap-2 border-t border-gray-100 dark:border-zinc-800/60 pt-3 mt-2"&gt;`  
  
                    `&lt;div class="flex items-center justify-between"&gt;`  
  
                        `&lt;span class="text-xs font-bold text-gray-700 dark:text-zinc-300"&gt;تعویض خودکار آیپی&lt;/span&gt;`  
  
                        `&lt;label class="relative inline-flex items-center cursor-pointer select-none"&gt;`  
  
                            `&lt;input type="checkbox" id="input-auto-rotate-ip-toggle" onchange="toggleAutoRotateIpInputs(this.checked)" class="sr-only peer"&gt;`  
  
                            `&lt;div class="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:bg-green-600 transition-colors after:content-[''] after:absolute after:top-[2px] after:right-[2px] after:bg-white after:rounded-full after:h-4 after:w-4 after:transition-transform peer-checked:after:-translate-x-[18px]"&gt;&lt;/div&gt;`  
  
                        `&lt;/label&gt;`  
  
                    `&lt;/div&gt;`  
  
                    `&lt;div id="auto-rotate-ip-inputs-container" class="hidden transition-all duration-300 pt-1"&gt;`  
  
                        `&lt;label class="block text-[11px] font-bold text-gray-500 dark:text-zinc-400 mb-1"&gt;زمان تعویض (دقیقه)&lt;/label&gt;`  
  
                        `&lt;input type="number" id="input-auto-rotate-ip-time" min="1" placeholder="توصیه شده 5" onblur="if(this.value === '' || parseInt(this.value) &lt; 1) this.value = '5';" class="w-full px-3 py-2.5 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-xs font-mono text-center" dir="ltr"&gt;`  
  
                    `&lt;/div&gt;`  
  
                `&lt;/div&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;div class="pt-4 flex gap-3"&gt;`  
  
                `&lt;button type="button" onclick="toggleIpSelectorModal(false)" class="flex-1 py-2 bg-transparent border-2 border-rose-700 text-rose-700 hover:bg-rose-900/20 hover:text-rose-800 dark:border-rose-700 dark:text-rose-500 dark:hover:bg-rose-900/40 dark:hover:text-rose-400 font-bold rounded-md text-xs transition shadow-sm"&gt;لغو&lt;/button&gt;`  
  
                `&lt;button type="button" onclick="applySelectedIps()" class="flex-1 py-2 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-medium rounded-md text-xs transition"&gt;دریافت&lt;/button&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;/div&gt;`  
  
`&lt;div id="proxy-selector-modal" class="fixed inset-0 z-[60] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out"&gt;`  
  
    `&lt;div class="w-full max-w-md bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md shadow-xl overflow-hidden transition-all transform duration-300 opacity-0 scale-95 ease-out"&gt;`  
  
        `&lt;div class="px-6 py-4 border-b border-gray-150 dark:border-amoled-border flex justify-between items-center bg-gray-50 dark:bg-zinc-900/50"&gt;`  
  
            `&lt;h3 class="font-bold text-gray-900 dark:text-zinc-100 text-sm"&gt;مخزن پـروکـسـی‌های آی‌پی ثابت&lt;/h3&gt;`  
  
            `&lt;button type="button" onclick="toggleProxySelectorModal(false)" class="p-1.5 rounded-md bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-900/50 text-red-600 dark:text-red-500 hover:bg-red-100 dark:hover:bg-red-900/50 transition-all duration-200 shadow-sm"&gt;`  
  
                `&lt;svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
            `&lt;/button&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;div class="p-5 space-y-4"&gt;`  
  
            `&lt;div class="p-4 bg-emerald-50 dark:bg-emerald-900/10 border border-emerald-200 dark:border-emerald-500/30 rounded-md relative"&gt;`  
  
                `&lt;h4 class="text-[13px] font-black text-emerald-700 dark:text-emerald-400 mb-2 flex items-center gap-1.5"&gt;`  
  
                    `&lt;svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                    `پـروکـسـی‌های اختصاصی (VIP)`  
  
                `&lt;/h4&gt;`  
  
                `&lt;p class="text-[10px] text-emerald-600/80 dark:text-emerald-500/70 mb-3 leading-relaxed font-medium"&gt;`  
  
                    `پـروکـسـی‌های اهدایی از طرف کاربران. کیفیت بالا و بدون نیاز به اسکن.`  
  
                `&lt;/p&gt;`  
  
                `&lt;div class="flex flex-col sm:flex-row gap-2"&gt;`  
  
                    `&lt;select id="vip-country-select" class="flex-1 px-3 py-2 bg-white dark:bg-amoled-input border border-emerald-200 dark:border-emerald-800/50 rounded-md text-xs focus:outline-none focus:ring-2 focus:ring-emerald-500 text-gray-700 dark:text-zinc-300 cursor-pointer"&gt;`  
  
                        `&lt;option value=""&gt;در حال بررسی مخزن...&lt;/option&gt;`  
  
                    `&lt;/select&gt;`  
  
                    `&lt;button type="button" onclick="loadVipProxy()" id="vip-fetch-btn" class="sm:w-auto w-full px-4 py-2 bg-transparent border-2 border-emerald-600 text-emerald-700 hover:bg-emerald-900/20 hover:text-emerald-800 dark:border-emerald-500 dark:text-emerald-500 dark:hover:bg-emerald-900/40 dark:hover:text-emerald-400 font-bold rounded-md text-xs transition shadow-sm disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap" disabled&gt;`  
  
						`دریافت`  
  
					`&lt;/button&gt;`  
  
                `&lt;/div&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;div class="relative py-1 flex items-center justify-center"&gt;`  
  
                `&lt;span class="absolute w-full border-t border-gray-200 dark:border-zinc-800"&gt;&lt;/span&gt;`  
  
                `&lt;span class="bg-white dark:bg-amoled-card px-3 text-[10px] font-bold text-gray-400 relative"&gt;یا اسکن عمومی&lt;/span&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;div class="p-4 bg-gray-50 dark:bg-zinc-900/40 border border-gray-200 dark:border-amoled-border rounded-md"&gt;`  
  
                `&lt;h4 class="text-[13px] font-black text-gray-700 dark:text-zinc-300 mb-2 flex items-center gap-1.5"&gt;`  
  
                    `&lt;svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                    `پـروکـسـی های عمومی`  
  
                `&lt;/h4&gt;`  
  
                `&lt;p class="text-[10px] text-gray-500 dark:text-zinc-500 mb-3 leading-relaxed font-medium"&gt;`  
  
                    `جستجو در منابع رایگان؛ به دلیل نیاز به تست کیفیت زمان‌بر است.`  
  
                `&lt;/p&gt;`  
  
                `&lt;div id="proxy-loading-state" class="text-center text-[11px] text-blue-500 font-bold hidden my-3 whitespace-pre-line leading-relaxed"&gt;`  
  
                    `در حال اسکن...`  
  
                `&lt;/div&gt;`  
  
                `&lt;div id="proxy-selection-form" class="flex flex-col gap-2"&gt;`  
  
                    `&lt;select id="proxy-country-select" class="w-full px-3 py-2 bg-white dark:bg-amoled-input border border-gray-300 dark:border-zinc-700 rounded-md text-xs focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-700 dark:text-zinc-300 cursor-pointer"&gt;`  
  
                        `&lt;option value=""&gt;در حال آماده‌سازی...&lt;/option&gt;`  
  
                    `&lt;/select&gt;`  
  
                    `&lt;button type="button" onclick="fetchAndLoadProxy()" id="proxy-fetch-btn" class="w-full py-2.5 bg-transparent border-2 border-blue-600 text-blue-700 hover:bg-blue-900/20 hover:text-blue-800 dark:border-blue-500 dark:text-blue-500 dark:hover:bg-blue-900/40 dark:hover:text-blue-400 font-bold rounded-md text-xs transition shadow-sm disabled:opacity-50 disabled:cursor-not-allowed" disabled&gt;`  
  
						`شروع اسکن و یافتن پـروکـسـی`  
  
					`&lt;/button&gt;`  
  
                `&lt;/div&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;div class="pt-1"&gt;`  
  
				`&lt;button type="button" onclick="toggleProxySelectorModal(false)" class="w-full py-2.5 bg-transparent border-2 border-rose-700 text-rose-700 hover:bg-rose-900/20 hover:text-rose-800 dark:border-rose-700 dark:text-rose-500 dark:hover:bg-rose-900/40 dark:hover:text-rose-400 font-bold rounded-md text-xs transition shadow-sm"&gt;انصراف و بستن&lt;/button&gt;`  
  
			`&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;/div&gt;`  
  
`&lt;div id="donate-modal" class="fixed inset-0 z-[60] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out"&gt;`  
  
    `&lt;div class="w-full max-w-sm bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md shadow-xl overflow-hidden transition-all transform duration-300 opacity-0 scale-95 ease-out" id="donate-modal-card"&gt;`  
  
        `&lt;div class="px-6 py-4 border-b border-gray-150 dark:border-amoled-border flex justify-between items-center bg-gray-50 dark:bg-zinc-900/50"&gt;`  
  
            `&lt;h3 class="font-bold text-gray-900 dark:text-zinc-100 text-sm"&gt;🎁 اهدای پـروکـسـی&lt;/h3&gt;`  
  
            `&lt;button type="button" onclick="toggleDonateModal(false)" class="p-1.5 rounded-md bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-900/50 text-red-600 dark:text-red-500 hover:bg-red-100 dark:hover:bg-red-900/50 transition-all duration-200 shadow-sm"&gt;`  
  
                `&lt;svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
            `&lt;/button&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;div class="p-6 space-y-4"&gt;`  
  
            `&lt;p class="text-[11px] text-gray-600 dark:text-zinc-400 leading-relaxed font-medium"&gt;`  
  
                `اگر سرور دارید میتونید با دکمه &lt;span class="text-blue-600 dark:text-blue-400 font-black"&gt;«ساخت پـروکـسـی شخصی»&lt;/span&gt; یک پـروکـسـی بسازید و اهدا کنید به پروژه`  
  
            `&lt;/p&gt;`  
  
            `&lt;div&gt;`  
  
                `&lt;input type="text" id="donate-proxy-input" placeholder="user:pass@ip:port" dir="ltr" class="w-full px-3 py-2.5 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-emerald-500 text-xs font-mono text-left text-gray-900 dark:text-zinc-100 transition"&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;div class="w-full text-center"&gt;`  
  
                `&lt;span id="donate-result" class="inline-block mt-1 text-[11px] font-bold transition-colors break-words leading-relaxed empty:hidden"&gt;&lt;/span&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;div class="pt-2 flex gap-3"&gt;`  
  
                `&lt;button type="button" onclick="toggleDonateModal(false)" class="flex-1 py-2 bg-transparent border-2 border-rose-700 text-rose-700 hover:bg-rose-900/20 hover:text-rose-800 dark:border-rose-700 dark:text-rose-500 dark:hover:bg-rose-900/40 dark:hover:text-rose-400 font-bold rounded-md text-xs transition shadow-sm"&gt;لغو&lt;/button&gt;`  
  
                `&lt;button type="button" id="donate-submit-btn" onclick="testAndDonateProxy()" class="flex-1 py-2 bg-transparent border-2 border-emerald-600 text-emerald-700 hover:bg-emerald-900/20 hover:text-emerald-800 dark:border-emerald-500 dark:text-emerald-500 dark:hover:bg-emerald-900/40 dark:hover:text-emerald-400 font-bold rounded-md text-xs transition shadow-sm"&gt;تست و اهدا&lt;/button&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;/div&gt;`  
  
`&lt;div id="support-modal" class="fixed inset-0 z-[105] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out"&gt;`  
  
    `&lt;div class="w-full max-w-md bg-white dark:bg-amoled-card border border-red-500/50 rounded-md shadow-2xl overflow-hidden p-6 text-center transition-all transform duration-300 opacity-0 scale-95 ease-out"&gt;`  
  
        `&lt;div class="inline-flex items-center justify-center w-16 h-16 rounded-full bg-red-100 dark:bg-red-900/30 text-red-500 mb-4 shadow-inner"&gt;`  
  
            `&lt;svg class="w-8 h-8 animate-pulse" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"&gt;`  
  
                `&lt;path stroke-linecap="round" stroke-linejoin="round" d="M21 8.25c0-2.485-2.099-4.5-4.688-4.5-1.935 0-3.597 1.126-4.312 2.733-.715-1.607-2.377-2.733-4.313-2.733C5.1 3.75 3 5.765 3 8.25c0 7.22 9 12 9 12s9-4.78 9-12z" /&gt;`  
  
            `&lt;/svg&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;h3 class="font-black text-xl text-gray-900 dark:text-white mb-3"&gt;حمایت از ما‌که‌وصلیم&lt;/h3&gt;`  
  
        `&lt;p class="text-sm text-gray-600 dark:text-gray-400 mb-6 leading-relaxed font-medium"&gt;`  
  
            `این پروژه متن باز و رایگان است. برای تضمین پایداری و ادامه مسیر توسعه، نیازمند همراهی و حمایت شما عزیزان هستم. هرگونه حمایت شما، انگیزه من را برای ارائه امکانات بهتر دوچندان می‌کند. ❤️`  
  
        `&lt;/p&gt;`  
  
        `&lt;div class="space-y-3"&gt;`  
  
            `&lt;a href="[https://donatonion.ir-netlify.workers.dev/](https://donatonion.ir-netlify.workers.dev/)" target="_blank" class="w-full py-3 bg-transparent border-2 border-orange-500 text-orange-600 hover:bg-orange-50 dark:border-orange-500/60 dark:text-orange-400 dark:hover:bg-orange-500/10 font-bold rounded-md text-sm transition duration-300 shadow-sm flex items-center justify-center gap-2"&gt;`  
  
                `&lt;svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"&gt;&lt;path d="M12 2a10 10 0 100 20 10 10 0 000-20zm0 18a8 8 0 110-16 8 8 0 010 16zm-.75-3.25h1.5v-1.5h-1.5v1.5zm0-3.5h1.5v-3h-1.5v3z"/&gt;&lt;/svg&gt;`  
  
                `حمایت مالی (رمز ارز)`  
  
            `&lt;/a&gt;`  
  
			`&lt;a href="[https://t.me/makevaslim4](https://t.me/makevaslim4)" target="_blank" class="w-full py-3 bg-transparent border-2 border-blue-500 text-blue-600 hover:bg-blue-50 dark:border-blue-500/60 dark:text-blue-400 dark:hover:bg-blue-500/10 font-bold rounded-md text-sm transition duration-300 shadow-sm flex items-center justify-center gap-2"&gt;`  
  
				`&lt;svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"&gt;&lt;path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.056 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.48.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z"/&gt;&lt;/svg&gt;`  
  
				`بوست تلگرام`  
  
			`&lt;/a&gt;`  
  
            `&lt;a href="[https://github.com/mkvaslim44/Ma_ke_vaslim-Panel](https://github.com/mkvaslim44/Ma_ke_vaslim-Panel)" target="_blank" class="w-full py-3 bg-transparent border-2 border-gray-600 text-gray-700 hover:bg-gray-100 dark:border-gray-500 dark:text-gray-300 dark:hover:bg-zinc-800 font-bold rounded-md text-sm transition duration-300 shadow-sm flex items-center justify-center gap-2"&gt;`  
  
                `&lt;svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"&gt;&lt;path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12"/&gt;&lt;/svg&gt;`  
  
                `ستاره در گیت‌هاب`  
  
            `&lt;/a&gt;`  
  
        `&lt;/div&gt;`  
  
            `&lt;button onclick="toggleSupportModal(false)" class="mt-4 w-full py-2.5 bg-transparent text-red-500 hover:text-red-600 hover:bg-red-50 dark:text-red-400 dark:hover:text-red-300 dark:hover:bg-red-900/20 font-bold rounded-md text-sm transition duration-300"&gt;`  
  
                `بستن`  
  
            `&lt;/button&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
    `&lt;div id="settings-modal" class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out"&gt;`  
  
        `&lt;div class="w-full max-w-md bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md shadow-xl overflow-hidden transition-all transform duration-300 opacity-0 scale-95 ease-out flex flex-col max-h-[90vh]"&gt;`  
  
            `&lt;div class="px-6 py-4 border-b border-gray-150 dark:border-amoled-border flex justify-between items-center bg-gray-50 dark:bg-zinc-900/50"&gt;`  
  
                `&lt;h3 class="font-bold text-gray-900 dark:text-zinc-100"&gt;تنظیمات پـنـل&lt;/h3&gt;`  
  
                `&lt;button onclick="toggleSettingsModal(false)" class="p-1.5 rounded-md bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-900/50 text-red-600 dark:text-red-500 hover:bg-red-100 dark:hover:bg-red-900/50 transition-all duration-200 shadow-sm"&gt;`  
  
                    `&lt;svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                `&lt;/button&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;div class="p-6 space-y-4 overflow-y-auto flex-1 overscroll-contain"&gt;`  
  
                `&lt;div class="pt-2"&gt;`  
  
					`&lt;label class="block text-sm font-medium mb-1.5 text-gray-700 dark:text-zinc-300"&gt;نرخ رفرش خودکار پـنـل&lt;/label&gt;`  
  
                    `&lt;div class="relative"&gt;`  
  
                        `&lt;select id="refresh-rate-select" onchange="changeRefreshRate(this.value)" class="w-full pl-8 pr-3 py-2.5 bg-white dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-700 dark:text-zinc-200 cursor-pointer appearance-none"&gt;`  
  
                            `&lt;option value="1000"&gt;۱ ثانیه&lt;/option&gt;`  
  
                            `&lt;option value="2000"&gt;۲ ثانیه&lt;/option&gt;`  
  
                            `&lt;option value="5000"&gt;۵ ثانیه&lt;/option&gt;`  
  
                            `&lt;option value="10000" selected&gt;۱۰ ثانیه (پیش‌فرض)&lt;/option&gt;`  
  
                            `&lt;option value="30000"&gt;۳۰ ثانیه&lt;/option&gt;`  
  
                            `&lt;option value="60000"&gt;۱ دقیقه&lt;/option&gt;`  
  
                            `&lt;option value="300000"&gt;۵ دقیقه&lt;/option&gt;`  
  
                            `&lt;option value="600000"&gt;۱۰ دقیقه&lt;/option&gt;`  
  
                        `&lt;/select&gt;`  
  
                        `&lt;div class="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3 text-gray-500 dark:text-zinc-400"&gt;`  
  
                            `&lt;svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                        `&lt;/div&gt;`  
  
                    `&lt;/div&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;div class="pt-4 border-t-2 border-gray-300 dark:border-zinc-700"&gt;`  
  
                    `&lt;h4 class="text-sm font-bold mb-3 text-gray-800 dark:text-zinc-200"&gt;🔒 تغییر رمز عبور مدیریت&lt;/h4&gt;`  
  
                    `&lt;div class="space-y-3"&gt;`  
  
                        `&lt;div&gt;`  
  
                            `&lt;label class="block text-[11px] text-gray-500 dark:text-gray-400 font-medium mb-1"&gt;رمز عبور فعلی&lt;/label&gt;`  
  
                            `&lt;input type="password" id="change-pwd-current" class="w-full px-3 py-2 bg-white dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-xs font-mono text-center"&gt;`  
  
                        `&lt;/div&gt;`  
  
                        `&lt;div&gt;`  
  
                            `&lt;label class="block text-[11px] text-gray-500 dark:text-gray-400 font-medium mb-1"&gt;رمز عبور جدید&lt;/label&gt;`  
  
                            `&lt;input type="password" id="change-pwd-new" class="w-full px-3 py-2 bg-white dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-xs font-mono text-center"&gt;`  
  
                        `&lt;/div&gt;`  
  
                        `&lt;button type="button" onclick="changeAdminPassword()" id="change-pwd-btn" class="w-full py-2 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-semibold rounded-md text-xs transition-all shadow-sm"&gt;تغییر رمز عبور&lt;/button&gt;`  
  
                    `&lt;/div&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;div class="pt-4 border-t-2 border-gray-300 dark:border-zinc-700"&gt;`  
  
                    `&lt;h4 class="text-sm font-bold mb-3 text-gray-800 dark:text-zinc-200"&gt;💾 پشتیبان‌گیری و بازیابی&lt;/h4&gt;`  
  
                    `&lt;div class="grid grid-cols-2 gap-3"&gt;`  
  
                        `&lt;button type="button" onclick="exportUsersBackup()" class="py-2.5 bg-transparent border-2 border-orange-500 text-orange-600 hover:bg-orange-50 dark:text-orange-400 dark:border-orange-500/60 dark:hover:bg-orange-500/10 rounded-md text-xs font-bold transition flex items-center justify-center gap-1.5 shadow-sm"&gt;`  
  
                            `📤 پشتیبان گیری`  
  
                        `&lt;/button&gt;`  
  
                        `&lt;button type="button" onclick="triggerImportBackup()" class="py-2.5 bg-transparent border-2 border-blue-500 text-blue-600 hover:bg-blue-50 dark:text-blue-400 dark:border-blue-500/60 dark:hover:bg-blue-500/10 rounded-md text-xs font-bold transition flex items-center justify-center gap-1.5 shadow-sm"&gt;`  
  
                            `📥 بازیابی`  
  
                        `&lt;/button&gt;`  
  
                    `&lt;/div&gt;`  
  
                    `&lt;input type="file" id="backup-file-input" onchange="importUsersBackup(event)" accept=".json" class="hidden"&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;div class="pt-4 flex gap-3"&gt;`  
  
                    `&lt;button type="button" onclick="toggleSettingsModal(false)" class="flex-1 py-2 bg-transparent border-2 border-rose-700 text-rose-700 hover:bg-rose-900/20 hover:text-rose-800 dark:border-rose-700 dark:text-rose-500 dark:hover:bg-rose-900/40 dark:hover:text-rose-400 font-bold rounded-md text-sm transition shadow-sm"&gt;انصراف&lt;/button&gt;`  
  
                    `&lt;button type="button" onclick="saveSettings()" id="save-settings-btn" class="flex-1 py-2 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-medium rounded-md text-sm transition"&gt;ذخیره تنظیمات&lt;/button&gt;`  
  
                `&lt;/div&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;div id="update-modal" class="fixed inset-0 z-[90] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out"&gt;`  
  
    `&lt;div class="w-full max-w-md bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md shadow-2xl overflow-hidden p-6 text-center transition-all transform duration-300 opacity-0 scale-95 ease-out"&gt;`  
  
        `&lt;div class="inline-flex items-center justify-center w-16 h-16 rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-500 mb-4 shadow-inner"&gt;`  
  
            `&lt;svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;h3 class="font-black text-xl text-gray-900 dark:text-white mb-2"&gt;بروزرسانی پـنـل&lt;/h3&gt;`  
  
        `&lt;p id="update-modal-text" class="text-sm text-gray-600 dark:text-gray-400 mb-6 leading-relaxed font-medium"&gt;`  
  
            `نسخه جدید در دسترس است. اگر آپدیت خودکار جواب نداد، حتماً از طریق لینک زیر آپدیت دستی را انجام دهید.`  
  
        `&lt;/p&gt;`  
  
        `&lt;div class="space-y-3"&gt;`  
  
            `&lt;button onclick="applyUpdate()" class="w-full py-3.5 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-black rounded-md text-sm transition duration-300 shadow-sm flex items-center justify-center gap-2"&gt;`  
  
                `&lt;svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                `آپدیت خودکار (توصیه شده)`  
  
            `&lt;/button&gt;`  
  
            `&lt;div class="relative py-2"&gt;`  
  
                `&lt;div class="absolute inset-0 flex items-center"&gt;`  
  
                    `&lt;div class="w-full border-t border-gray-200 dark:border-zinc-800"&gt;&lt;/div&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;div class="relative flex justify-center text-xs"&gt;`  
  
                    `&lt;span class="bg-white dark:bg-amoled-card px-2 text-gray-400"&gt;یا&lt;/span&gt;`  
  
                `&lt;/div&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;a href="[https://t.me/makevaslim4](https://t.me/makevaslim4)" target="_blank" class="w-full py-3.5 bg-orange-50 dark:bg-orange-950/30 hover:bg-orange-100 dark:hover:bg-orange-900/50 text-orange-600 dark:text-orange-500 border border-orange-300 dark:border-orange-500 font-bold rounded-md text-sm transition duration-300 shadow-sm flex items-center justify-center gap-2"&gt;`  
  
                `&lt;svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;`  
  
                    `&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"&gt;&lt;/path&gt;`  
  
                `&lt;/svg&gt;`  
  
                `آپدیت از طریق ربات`  
  
            `&lt;/a&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;button onclick="toggleUpdateModal(false)" class="mt-5 w-full py-3.5 bg-transparent border-2 border-rose-700 text-rose-700 hover:bg-rose-900/20 hover:text-rose-800 dark:border-rose-700 dark:text-rose-500 dark:hover:bg-rose-900/40 dark:hover:text-rose-400 font-bold rounded-md text-sm transition duration-300 shadow-sm flex items-center justify-center"&gt;`  
  
            `انصراف`  
  
        `&lt;/button&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;/div&gt;`  
  
	`&lt;div id="token-modal" class="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/70 opacity-0 pointer-events-none transition-opacity duration-200 ease-out"&gt;`  
  
        `&lt;div id="token-modal-card" class="w-full max-w-md bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md shadow-2xl p-6 transform transition-all scale-95 opacity-0 duration-200"&gt;`  
  
            `&lt;div class="flex justify-between items-center mb-6"&gt;`  
  
                `&lt;div class="flex items-center gap-2"&gt;`  
  
                    `&lt;div class="w-2.5 h-2.5 rounded-full bg-orange-500"&gt;&lt;/div&gt;`  
  
                    `&lt;h3 class="text-lg font-bold text-gray-900 dark:text-white"&gt;تنظیم توکن کلودفلر&lt;/h3&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;button onclick="toggleTokenModal(false)" class="p-1.5 rounded-md bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-900/50 text-red-600 dark:text-red-500 hover:bg-red-100 dark:hover:bg-red-900/50 transition-all duration-200 shadow-sm"&gt;`  
  
                    `&lt;svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                `&lt;/button&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;div class="mb-5 p-3 bg-orange-50 dark:bg-orange-900/20 border border-orange-200 dark:border-orange-800/50 rounded-md text-xs leading-relaxed text-orange-800 dark:text-orange-300 font-medium"&gt;`  
  
                `توکن کلودفلر شما در این پـنـل ذخیره نشده است. برای فعال‌سازی آپدیت خودکار از داخل پـنـل، لطفاً توکن خود را دریافت کرده و در کادر زیر وارد کنید.`  
  
            `&lt;/div&gt;`  
  
            `&lt;a href="[https://dash.cloudflare.com/profile/api-tokens?permissionGroupKeys=%5B%7B%22key%22%3A%22workers_scripts%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22workers_kv_storage%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22d1%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22account_settings%22%2C%22type%22%3A%22read%22%7D%2C%7B%22key%22%3A%22workers_subdomain%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22account_analytics%22%2C%22type%22%3A%22read%22%7D%5D&amp;accountId=*&amp;zoneId=all&amp;name=Zeus-Deployer-Token](https://dash.cloudflare.com/profile/api-tokens?permissionGroupKeys=%5B%7B%22key%22%3A%22workers_scripts%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22workers_kv_storage%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22d1%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22account_settings%22%2C%22type%22%3A%22read%22%7D%2C%7B%22key%22%3A%22workers_subdomain%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22account_analytics%22%2C%22type%22%3A%22read%22%7D%5D&accountId=*&zoneId=all&name=Zeus-Deployer-Token)" target="_blank" class="flex items-center justify-center gap-2 w-full py-3 bg-[#d94800] hover:bg-[#e35802] text-white font-bold rounded-md text-sm transition duration-300 mb-4 shadow-md shadow-orange-500/20"&gt;`  
  
                `&lt;svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
                `دریافت توکن کلودفلر`  
  
            `&lt;/a&gt;`  
  
            `&lt;div class="space-y-4"&gt;`  
  
                `&lt;input type="password" id="update-token-input" placeholder="توکن را اینجا وارد کنید" class="w-full px-4 py-3 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-md focus:outline-none focus:ring-2 focus:ring-orange-500 text-sm font-mono text-center text-gray-900 dark:text-zinc-100 transition" dir="auto"&gt;`  
  
                `&lt;button id="submit-token-btn" onclick="submitTokenForUpdate()" class="w-full py-3 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-bold rounded-md text-sm transition duration-300 shadow-lg"&gt;`  
  
                    `ثبت و آپدیت پـنـل`  
  
                `&lt;/button&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;div id="qr-modal" class="fixed inset-0 z-[110] flex items-center justify-center p-4 bg-black/70 opacity-0 pointer-events-none transition-opacity duration-200 ease-out"&gt;`  
  
    `&lt;div id="qr-modal-card" class="w-full max-w-sm bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md shadow-2xl p-6 transform transition-all scale-95 opacity-0 duration-200 text-center"&gt;`  
  
        `&lt;div class="flex justify-between items-center mb-4"&gt;`  
  
            `&lt;h3 class="text-lg font-bold text-gray-900 dark:text-white"&gt;QR Code&lt;/h3&gt;`  
  
            `&lt;button onclick="toggleQrModal(false)" class="p-1.5 rounded-md bg-red-50 dark:bg-red-950/30 border border-red-200 dark:border-red-900/50 text-red-600 dark:text-red-500 hover:bg-red-100 dark:hover:bg-red-900/50 transition-all duration-200 shadow-sm"&gt;`  
  
                `&lt;svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
            `&lt;/button&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;div class="flex justify-center bg-white p-4 rounded-md mb-4"&gt;`  
  
            `&lt;div id="qrcode-container"&gt;&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;/div&gt;`  
  
    `&lt;div id="bulk-actions-bar" class="fixed bottom-4 left-1/2 -translate-x-1/2 z-[40] bg-white dark:bg-zinc-900/90 border border-gray-200 dark:border-zinc-800/80 px-6 py-4 rounded-md shadow-2xl flex flex-wrap items-center justify-between gap-4 w-[95%] max-w-4xl transition-all duration-300 transform translate-y-28 opacity-0 pointer-events-none backdrop-blur-md"&gt;`  
  
        `&lt;div class="flex items-center gap-2"&gt;`  
  
            `&lt;span class="w-3 h-3 bg-blue-500 rounded-full animate-pulse shadow-sm shadow-blue-500/50"&gt;&lt;/span&gt;`  
  
            `&lt;span id="bulk-selected-count" class="text-sm font-bold text-gray-800 dark:text-zinc-200"&gt;۰ کاربر انتخاب شده&lt;/span&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;div class="flex flex-wrap gap-2 justify-end"&gt;`  
  
            `&lt;button onclick="bulkToggleStatus(1)" class="px-3 py-1.5 bg-green-50 dark:bg-green-950/20 text-green-700 dark:text-green-500 hover:bg-green-100 dark:hover:bg-green-900/30 rounded-md text-xs font-bold transition border border-green-200 dark:border-green-900/50 flex items-center gap-1"&gt;`  
  
                `✅ فعال‌سازی`  
  
            `&lt;/button&gt;`  
  
            `&lt;button onclick="bulkToggleStatus(0)" class="px-3 py-1.5 bg-amber-50 dark:bg-amber-950/20 text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/30 rounded-md text-xs font-bold transition border border-amber-200 dark:border-amber-900/50 flex items-center gap-1"&gt;`  
  
                `❌ غیرفعال‌سازی`  
  
            `&lt;/button&gt;`  
  
            `&lt;button onclick="bulkReset('volume')" class="px-3 py-1.5 bg-blue-50 dark:bg-blue-950/20 text-blue-600 dark:text-blue-400 hover:bg-blue-100 dark:hover:bg-blue-900/30 rounded-md text-xs font-bold transition border border-blue-200 dark:border-blue-900/50 flex items-center gap-1"&gt;`  
  
                `📊 ریست حجم`  
  
            `&lt;/button&gt;`  
  
            `&lt;button onclick="bulkReset('req')" class="px-3 py-1.5 bg-sky-50 dark:bg-sky-950/20 text-sky-600 dark:text-sky-400 hover:bg-sky-100 dark:hover:bg-sky-900/30 rounded-md text-xs font-bold transition border border-sky-200 dark:border-sky-900/50 flex items-center gap-1"&gt;`  
  
                `⚡ ریست ریکوئست`  
  
            `&lt;/button&gt;`  
  
            `&lt;button onclick="bulkReset('time')" class="px-3 py-1.5 bg-purple-50 dark:bg-purple-950/20 text-purple-600 dark:text-purple-400 hover:bg-purple-100 dark:hover:bg-purple-900/30 rounded-md text-xs font-bold transition border border-purple-200 dark:border-purple-900/50 flex items-center gap-1"&gt;`  
  
                `⏳ ریست زمان`  
  
            `&lt;/button&gt;`  
  
            `&lt;button onclick="bulkDelete()" class="px-3 py-1.5 bg-red-50 dark:bg-red-950/30 text-red-600 dark:text-red-450 hover:bg-red-100 dark:hover:bg-red-900/40 rounded-md text-xs font-bold transition border border-red-200 dark:border-red-900/50 flex items-center gap-1"&gt;`  
  
                `🗑️ حذف گروهی`  
  
            `&lt;/button&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
	`&lt;div id="update-success-modal" class="fixed inset-0 z-[120] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out"&gt;`  
  
		`&lt;div class="w-full max-w-md bg-white dark:bg-amoled-card border border-green-600/50 rounded-md shadow-2xl overflow-hidden p-6 text-center transition-all transform duration-300 opacity-0 scale-95 ease-out"&gt;`  
  
			`&lt;div class="inline-flex items-center justify-center w-16 h-16 rounded-full bg-green-100 dark:bg-green-900/30 text-green-600 mb-4 shadow-inner"&gt;`  
  
				`&lt;svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"&gt;&lt;/path&gt;&lt;/svg&gt;`  
  
			`&lt;/div&gt;`  
  
			`&lt;h3 class="font-black text-xl text-gray-900 dark:text-white mb-2"&gt;آپدیت موفقیت‌آمیز&lt;/h3&gt;`  
  
			`&lt;p class="text-sm text-gray-600 dark:text-gray-400 mb-6 leading-relaxed font-medium"&gt;`  
  
				`آپدیت موفق بود لطفا صفحه را 10 ثانیه دیگر رفرش کنید تا نسخه جدید لود شود`  
  
			`&lt;/p&gt;`  
  
			`&lt;button onclick="window.location.reload()" class="w-full py-3.5 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-black rounded-md text-sm transition duration-300 shadow-lg"&gt;`  
  
				`رفرش صفحه`  
  
			`&lt;/button&gt;`  
  
		`&lt;/div&gt;`  
  
	`&lt;/div&gt;`  
  
`${COMMON_TOAST_HTML}`  
  
`&lt;div id="custom-confirm-modal" class="fixed inset-0 z-[9999] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out"&gt;`  
  
    `&lt;div id="custom-confirm-card" class="w-full max-w-sm bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md shadow-2xl overflow-hidden p-6 text-center transform transition-all scale-95 duration-300"&gt;`  
  
        `&lt;h3 class="font-black text-xl text-gray-900 dark:text-white mb-3"&gt;تأیید عملیات&lt;/h3&gt;`  
  
        `&lt;p id="custom-confirm-message" class="text-sm text-gray-600 dark:text-gray-400 mb-6 leading-relaxed font-medium"&gt;&lt;/p&gt;`  
  
        `&lt;div class="flex gap-3"&gt;`  
  
            `&lt;button id="custom-confirm-cancel" class="flex-1 py-3 bg-transparent border-2 border-rose-700 text-rose-700 hover:bg-rose-900/20 hover:text-rose-800 dark:border-rose-700 dark:text-rose-500 dark:hover:bg-rose-900/40 dark:hover:text-rose-400 font-bold rounded-md text-sm transition duration-200 shadow-sm"&gt;انصراف&lt;/button&gt;`  
  
            `&lt;button id="custom-confirm-ok" class="flex-1 py-3 bg-transparent border-2 border-green-600 text-green-700 hover:bg-green-900/20 hover:text-green-800 dark:border-green-500 dark:text-green-500 dark:hover:bg-green-900/40 dark:hover:text-green-400 font-bold rounded-md text-sm transition duration-200 shadow-lg"&gt;تأیید&lt;/button&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;/div&gt;`  
  
    `&lt;script&gt;`  
  
		`async function fetchWithFallbackUI(path, options = {}) {`  
  
			`const githubUrl = '[https://raw.githubusercontent.com/zeus-panel/ZEUS-PANEL/main/](https://raw.githubusercontent.com/zeus-panel/ZEUS-PANEL/main/)' + path;`  
  
			`const staticUrl = '[https://zeus-files.surge.sh/](https://zeus-files.surge.sh/)' + path;`  
  
			`try {`  
  
				`const res = await fetch(githubUrl, options);`  
  
				`if (res.ok) return res;`  
  
			`} catch (e) {}`  
  
			`return await fetch(staticUrl, options);`  
  
		`}`  
  
		`function showToast(message, type = 'success') {`  
  
            `const container = document.getElementById('toast-container');`  
  
            `const toast = document.createElement('div');`  
  
            `const colors = type === 'error' `  
  
                `? 'bg-red-50 dark:bg-red-900/40 border-red-200 dark:border-red-800 text-red-600 dark:text-red-400' `  
  
                `: 'bg-green-50 dark:bg-green-900/40 border-green-200 dark:border-green-800 text-green-700 dark:text-green-500';`  
  
            `toast.className = 'px-4 py-3 border rounded-md shadow-lg font-bold text-sm transform transition-all duration-300 -translate-y-full opacity-0 ' + colors;`  
  
            `toast.innerText = message;`  
  
            `container.appendChild(toast);`  
  
            `requestAnimationFrame(() =&gt; {`  
  
                `toast.classList.remove('-translate-y-full', 'opacity-0');`  
  
            `});`  
  
            `setTimeout(() =&gt; {`  
  
                `toast.classList.add('-translate-y-full', 'opacity-0');`  
  
                `setTimeout(() =&gt; toast.remove(), 300);`  
  
            `}, 3000);`  
  
        `}`  
  
        `function customConfirm(message) {`  
  
            `return new Promise((resolve) =&gt; {`  
  
                `const modal = document.getElementById('custom-confirm-modal');`  
  
                `const card = document.getElementById('custom-confirm-card');`  
  
                `const msgEl = document.getElementById('custom-confirm-message');`  
  
                `const btnOk = document.getElementById('custom-confirm-ok');`  
  
                `const btnCancel = document.getElementById('custom-confirm-cancel');`  
  
                `msgEl.innerText = message;`  
  
                `modal.classList.remove('opacity-0', 'pointer-events-none');`  
  
                `modal.classList.add('opacity-100', 'pointer-events-auto');`  
  
                `card.classList.remove('scale-95');`  
  
                `card.classList.add('scale-100');`  
  
                `const cleanup = () =&gt; {`  
  
                    `modal.classList.remove('opacity-100', 'pointer-events-auto');`  
  
                    `modal.classList.add('opacity-0', 'pointer-events-none');`  
  
                    `card.classList.remove('scale-100');`  
  
                    `card.classList.add('scale-95');`  
  
                    `btnOk.removeEventListener('click', onOk);`  
  
                    `btnCancel.removeEventListener('click', onCancel);`  
  
                `};`  
  
                `const onOk = () =&gt; { cleanup(); resolve(true); };`  
  
                `const onCancel = () =&gt; { cleanup(); resolve(false); };`  
  
                `btnOk.addEventListener('click', onOk);`  
  
                `btnCancel.addEventListener('click', onCancel);`  
  
            `});`  
  
        `}`  
  
        `window.alert = function(message) {`  
  
            `const msgStr = message ? message.toString() : '';`  
  
            `if (msgStr.includes('خطا') || msgStr.includes('⚠️') || msgStr.includes('❌')) {`  
  
                `showToast(msgStr, 'error');`  
  
            `} else {`  
  
                `showToast(msgStr, 'success');`  
  
            `}`  
  
        `};`  
  
        `window.selectedUsernames = new Set();`  
  
        `function toggleSelectAllUsers(el) {`  
  
            `const checkboxes = document.querySelectorAll('input[name="select-user"]');`  
  
            `checkboxes.forEach(cb =&gt; {`  
  
                `cb.checked = el.checked;`  
  
                `const username = decodeURIComponent(cb.value);`  
  
                `if (el.checked) {`  
  
                    `window.selectedUsernames.add(username);`  
  
                `} else {`  
  
                    `window.selectedUsernames.delete(username);`  
  
                `}`  
  
            `});`  
  
            `updateBulkActionsBar();`  
  
        `}`  
  
        `function onUserSelectChange(el) {`  
  
            `const username = decodeURIComponent(el.value);`  
  
            `if (el.checked) {`  
  
                `window.selectedUsernames.add(username);`  
  
            `} else {`  
  
                `window.selectedUsernames.delete(username);`  
  
            `}`  
  
            `updateBulkActionsBar();`  
  
        `}`  
  
        `function updateBulkActionsBar() {`  
  
            `const bar = document.getElementById('bulk-actions-bar');`  
  
            `const countSpan = document.getElementById('bulk-selected-count');`  
  
            `const selectAllCheckbox = document.getElementById('select-all-users');`  
  
            `const selectedCount = window.selectedUsernames.size;`  
  
            `if (countSpan) {`  
  
                `countSpan.innerText = selectedCount + ' کاربر انتخاب شده';`  
  
            `}`  
  
            `const checkboxes = document.querySelectorAll('input[name="select-user"]');`  
  
            `if (checkboxes.length &gt; 0) {`  
  
                `const allChecked = Array.from(checkboxes).every(cb =&gt; cb.checked);`  
  
                `if (selectAllCheckbox) selectAllCheckbox.checked = allChecked;`  
  
            `} else {`  
  
                `if (selectAllCheckbox) selectAllCheckbox.checked = false;`  
  
            `}`  
  
            `if (selectedCount &gt; 0) {`  
  
                `bar.classList.remove('opacity-0', 'pointer-events-none', 'translate-y-28');`  
  
                `bar.classList.add('opacity-100', 'pointer-events-auto', 'translate-y-0');`  
  
            `} else {`  
  
                `bar.classList.remove('opacity-100', 'pointer-events-auto', 'translate-y-0');`  
  
                `bar.classList.add('opacity-0', 'pointer-events-none', 'translate-y-28');`  
  
            `}`  
  
        `}`  
  
        `async function bulkDelete() {`  
  
            `const usernames = Array.from(window.selectedUsernames);`  
  
            `if (usernames.length === 0) return;`  
  
            `if (await customConfirm('⚠️ آیا از حذف گروهی ' + usernames.length + ' کاربر انتخاب شده مطمئن هستید؟ این عمل غیرقابل بازگشت است.')) {`  
  
                `const bar = document.getElementById('bulk-actions-bar');`  
  
                `const buttons = bar.querySelectorAll('button');`  
  
                `buttons.forEach(btn =&gt; btn.disabled = true);`  
  
                `try {`  
  
                    `let successCount = 0;`  
  
                    `await Promise.all([usernames.map](http://usernames.map)(async (uname) =&gt; {`  
  
                        `try {`  
  
                            `const res = await fetch('/api/users/' + encodeURIComponent(uname), { method: 'DELETE' });`  
  
                            `if (res.ok) {`  
  
                                `successCount++;`  
  
                                `window.selectedUsernames.delete(uname);`  
  
                            `}`  
  
                        `} catch(e) {}`  
  
                    `}));`  
  
                    `alert('✅ عملیات حذف گروهی انجام شد. ' + successCount + ' کاربر با موفقیت حذف شدند.');`  
  
                `} finally {`  
  
                    `buttons.forEach(btn =&gt; btn.disabled = false);`  
  
                    `updateBulkActionsBar();`  
  
                    `await loadUsers(true);`  
  
                `}`  
  
            `}`  
  
        `}`  
  
        `async function bulkToggleStatus(targetActive) {`  
  
            `const usernames = Array.from(window.selectedUsernames);`  
  
            `if (usernames.length === 0) return;`  
  
            `const actionText = targetActive === 1 ? 'فعال‌سازی' : 'غیرفعال‌سازی';`  
  
            `if (await customConfirm('آیا از ' + actionText + ' گروهی ' + usernames.length + ' کاربر انتخاب شده مطمئن هستید؟')) {`  
  
                `const bar = document.getElementById('bulk-actions-bar');`  
  
                `const buttons = bar.querySelectorAll('button');`  
  
                `buttons.forEach(btn =&gt; btn.disabled = true);`  
  
                `try {`  
  
                    `let successCount = 0;`  
  
                    `await Promise.all([usernames.map](http://usernames.map)(async (uname) =&gt; {`  
  
                        `const user = window.allUsers.find(u =&gt; u.username === uname);`  
  
                        `if (!user) return;`  
  
                        `const isCurrentActive = [user.is](http://user.is)_active !== 0;`  
  
                        `const shouldToggle = (targetActive === 1 &amp;&amp; !isCurrentActive) || (targetActive === 0 &amp;&amp; isCurrentActive);`  
  
                        `if (shouldToggle) {`  
  
                            `try {`  
  
                                `const res = await fetch('/api/users/' + encodeURIComponent(uname), {`  
  
                                    `method: 'PUT',`  
  
                                    `headers: { 'Content-Type': 'application/json' },`  
  
                                    `body: JSON.stringify({ toggle_only: true })`  
  
                                `});`  
  
                                `if (res.ok) successCount++;`  
  
                            `} catch(e) {}`  
  
                        `} else {`  
  
                            `successCount++;`  
  
                        `}`  
  
                    `}));`  
  
                    `alert('✅ عملیات ' + actionText + ' با موفقیت برای تمامی کاربران واجد شرایط اعمال شد.');`  
  
                `} finally {`  
  
                    `buttons.forEach(btn =&gt; btn.disabled = false);`  
  
                    `updateBulkActionsBar();`  
  
                    `await loadUsers(true);`  
  
                `}`  
  
            `}`  
  
        `}`  
  
        `async function bulkReset(actionType) {`  
  
            `const usernames = Array.from(window.selectedUsernames);`  
  
            `if (usernames.length === 0) return;`  
  
            `let actionName = '';`  
  
            `if (actionType === 'volume') actionName = 'حجم مصرفی';`  
  
            `else if (actionType === 'req') actionName = 'تعداد ریکوئست‌ها';`  
  
            `else if (actionType === 'time') actionName = 'زمان اشتراک';`  
  
            `if (await customConfirm('آیا از ریست کردن گروهی ' + actionName + ' برای ' + usernames.length + ' کاربر انتخاب شده مطمئن هستید؟')) {`  
  
                `const bar = document.getElementById('bulk-actions-bar');`  
  
                `const buttons = bar.querySelectorAll('button');`  
  
                `buttons.forEach(btn =&gt; btn.disabled = true);`  
  
                `try {`  
  
                    `let successCount = 0;`  
  
                    `await Promise.all([usernames.map](http://usernames.map)(async (uname) =&gt; {`  
  
                        `try {`  
  
                            `const res = await fetch('/api/users/' + encodeURIComponent(uname), {`  
  
                                `method: 'PUT',`  
  
                                `headers: { 'Content-Type': 'application/json' },`  
  
                                `body: JSON.stringify({ reset_action: actionType })`  
  
                            `});`  
  
                            `if (res.ok) successCount++;`  
  
                        `} catch(e) {}`  
  
                    `}));`  
  
                    `alert('✅ عملیات ریست گروهی ' + actionName + ' با موفقیت برای ' + successCount + ' کاربر اعمال شد.');`  
  
                `} finally {`  
  
                    `buttons.forEach(btn =&gt; btn.disabled = false);`  
  
                    `updateBulkActionsBar();`  
  
                    `await loadUsers(true);`  
  
                `}`  
  
            `}`  
  
        `}`  
  
        `const tlsPorts = ['443', '2053', '2083', '2087', '2096', '8443'];`  
  
        `const nonTlsPorts = ['80', '8080', '8880', '2052', '2086', '2095'];`  
  
        `let isEditMode = false;`  
  
        `let editingUsername = '';`  
  
        `function renderPortCheckboxes() {`  
  
            `const tlsContainer = document.getElementById('tls-ports-list');`  
  
            `const nonTlsContainer = document.getElementById('nontls-ports-list');`  
  
            `tlsContainer.innerHTML = [tlsPorts.map](http://tlsPorts.map)(function(port) {`  
  
                `const isCheckedDefault = port === '443' ? 'checked' : '';`  
  
                `return '&lt;label class="relative cursor-pointer"&gt;' +`  
  
                    `'&lt;input type="checkbox" name="ports" value="' + port + '" ' + isCheckedDefault + ' class="peer sr-only"&gt;' +`  
  
                    `'&lt;div class="flex items-center justify-center gap-1 px-1.5 py-1.5 border border-gray-200 dark:border-zinc-800/80 rounded-md text-[11px] font-semibold select-none transition-all duration-200 hover:bg-gray-50 dark:hover:bg-zinc-800/40 text-gray-700 dark:text-zinc-300 peer-checked:bg-blue-50 dark:peer-checked:bg-blue-950/25 peer-checked:border-blue-500 dark:peer-checked:border-blue-500/70 peer-checked:text-blue-600 dark:peer-checked:text-blue-400 shadow-sm"&gt;' +`  
  
                        `'&lt;span&gt;' + port + '&lt;/span&gt;' +`  
  
                        `'&lt;svg class="w-3 h-3 hidden peer-checked:block text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"&gt;&lt;/path&gt;&lt;/svg&gt;' +`  
  
                    `'&lt;/div&gt;' +`  
  
                `'&lt;/label&gt;';`  
  
            `}).join('');`  
  
            `nonTlsContainer.innerHTML = [nonTlsPorts.map](http://nonTlsPorts.map)(function(port) {`  
  
                `const isCheckedDefault = port === '80' ? 'checked' : '';`  
  
                `return '&lt;label class="relative cursor-pointer"&gt;' +`  
  
                    `'&lt;input type="checkbox" name="ports" value="' + port + '" ' + isCheckedDefault + ' class="peer sr-only"&gt;' +`  
  
                    `'&lt;div class="flex items-center justify-center gap-1 px-1.5 py-1.5 border border-gray-200 dark:border-zinc-800/80 rounded-md text-[11px] font-semibold select-none transition-all duration-200 hover:bg-gray-50 dark:hover:bg-zinc-800/40 text-gray-700 dark:text-zinc-300 peer-checked:bg-amber-50 dark:peer-checked:bg-amber-950/25 peer-checked:border-amber-500 dark:peer-checked:border-amber-500/70 peer-checked:text-amber-600 dark:peer-checked:text-amber-400 shadow-sm"&gt;' +`  
  
                        `'&lt;span&gt;' + port + '&lt;/span&gt;' +`  
  
                        `'&lt;svg class="w-3 h-3 hidden peer-checked:block text-amber-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"&gt;&lt;/path&gt;&lt;/svg&gt;' +`  
  
                    `'&lt;/div&gt;' +`  
  
                `'&lt;/label&gt;';`  
  
            `}).join('');`  
  
        `}`  
  
        `setTimeout(function() {`  
  
            `const cb443 = document.querySelector('input[name="ports"][value="443"]');`  
  
            `if (cb443) cb443.checked = true;`  
  
            `const cb80 = document.querySelector('input[name="ports"][value="80"]');`  
  
            `if (cb80) cb80.checked = true;`  
  
        `}, 100);`  
  
        `function toggleSettingsModal(show) { setModalState('settings-modal', show); }`  
  
        `window.toggleAutoResetInputs = function(show) {`  
  
			`const container = document.getElementById('auto-reset-inputs-container');`  
  
			`const volInput = document.getElementById('input-auto-reset-vol');`  
  
			`const reqInput = document.getElementById('input-auto-reset-req');`  
  
			`if (container) {`  
  
				`if (show) {`  
  
					`container.classList.remove('opacity-50', 'pointer-events-none');`  
  
					`if (volInput) volInput.disabled = false;`  
  
					`if (reqInput) reqInput.disabled = false;`  
  
				`} else {`  
  
					`container.classList.add('opacity-50', 'pointer-events-none');`  
  
					`if (volInput) volInput.disabled = true;`  
  
					`if (reqInput) reqInput.disabled = true;`  
  
				`}`  
  
			`}`  
  
		`};`  
  
        `window.toggleAutoRotateIpInputs = function(show) {`  
  
			`const container = document.getElementById('auto-rotate-ip-inputs-container');`  
  
			`if (container) {`  
  
				`if (show) container.classList.remove('hidden');`  
  
				`else container.classList.add('hidden');`  
  
			`}`  
  
		`};`  
  
        `window.toggleFragInputs = function(show) {`  
  
            `const container = document.getElementById('frag-inputs-container');`  
  
            `if (container) {`  
  
                `if (show) {`  
  
                    `container.classList.remove('hidden');`  
  
                `} else {`  
  
                    `container.classList.add('hidden');`  
  
                `}`  
  
            `}`  
  
        `};`  
  
        `function toggleModal(show) {`  
  
            `setModalState('user-modal', show);`  
  
            `if (!show) {`  
  
                `isEditMode = false;`  
  
                `editingUsername = '';`  
  
                `document.getElementById('modal-title').innerText = 'ایجاد کاربر جدید';`  
  
                `document.getElementById('submit-btn').innerText = 'ایجاد کاربر';`  
  
                `document.getElementById('input-name').disabled = false;`  
  
                `document.getElementById('create-user-form').reset();`  
  
                `const cb443 = document.querySelector('input[name="ports"][value="443"]');`  
  
                `if (cb443) cb443.checked = true;`  
  
                `const cb80 = document.querySelector('input[name="ports"][value="80"]');`  
  
                `if (cb80) cb80.checked = true;`  
  
                `const fpSelect = document.getElementById('fingerprint-select');`  
  
                `if (fpSelect) fpSelect.value = 'ios';`  
  
                `const bpCheck = document.getElementById('input-block-porn');`  
  
                `if (bpCheck) bpCheck.checked = false;`  
  
                `const baCheck = document.getElementById('input-block-ads');`  
  
				`if (baCheck) baCheck.checked = false;`  
  
				`const autoRotateUserProxyCheck = document.getElementById('input-auto-rotate-user-proxy');`  
  
				`if (autoRotateUserProxyCheck) autoRotateUserProxyCheck.checked = false;`  
  
				`const fragLenInput = document.getElementById('input-frag-len');`  
  
				`if (fragLenInput) fragLenInput.value = '200-3000';`  
  
				`const fragIntInput = document.getElementById('input-frag-int');`  
  
				`if (fragIntInput) fragIntInput.value = '1-2';`  
  
                `const fragToggle = document.getElementById('input-frag-toggle');`  
  
                `if (fragToggle) fragToggle.checked = true;`  
  
                `window.toggleFragInputs(true);`  
  
				`const customPortInput = document.getElementById('input-custom-ports');`  
  
				`if (customPortInput) customPortInput.value = '';`  
  
				`document.getElementById('hidden-auto-rotate').value = '0';`  
  
				`document.getElementById('hidden-rotate-time').value = '';`  
  
				`document.getElementById('hidden-ip-operator').value = 'all';`  
  
				`document.getElementById('hidden-ip-count').value = '20';`  
  
				`const autoResetToggle = document.getElementById('input-auto-reset-toggle');`  
  
				`if (autoResetToggle) autoResetToggle.checked = false;`  
  
				`document.getElementById('input-auto-reset-vol').value = '';`  
  
				`document.getElementById('input-auto-reset-req').value = '';`  
  
				`window.toggleAutoResetInputs(false);`  
  
            `}`  
  
        `}`  
  
		`function toggleUpdateModal(show, version = '') {`  
  
            `if (show &amp;&amp; version) document.getElementById('update-modal-text').innerHTML = 'نسخه جدید (&lt;b&gt;v' + version + '&lt;/b&gt;) در دسترس است.&lt;br&gt;اگر آپدیت خودکار عمل نکرد لطفا از ربات استفاده کنید.';`  
  
            `setModalState('update-modal', show);`  
  
        `}`  
  
        `function openCreateModal() {`  
  
            `isEditMode = false;`  
  
            `editingUsername = '';`  
  
            `document.getElementById('modal-title').innerText = 'ایجاد کاربر جدید';`  
  
            `document.getElementById('submit-btn').innerText = 'ایجاد کاربر';`  
  
            `document.getElementById('input-name').disabled = false;`  
  
            `document.getElementById('create-user-form').reset();`  
  
            `const cb443 = document.querySelector('input[name="ports"][value="443"]');`  
  
            `if (cb443) cb443.checked = true;`  
  
            `const cb80 = document.querySelector('input[name="ports"][value="80"]');`  
  
            `if (cb80) cb80.checked = true;`  
  
            `const fpSelect = document.getElementById('fingerprint-select');`  
  
            `if (fpSelect) fpSelect.value = 'ios';`  
  
            `const fragToggle = document.getElementById('input-frag-toggle');`  
  
            `if (fragToggle) fragToggle.checked = true;`  
  
            `window.toggleFragInputs(true);`  
  
			`const autoResetToggle = document.getElementById('input-auto-reset-toggle');`  
  
			`if (autoResetToggle) autoResetToggle.checked = false;`  
  
			`document.getElementById('input-auto-reset-vol').value = '';`  
  
			`document.getElementById('input-auto-reset-req').value = '';`  
  
			`window.toggleAutoResetInputs(false);`  
  
			`const blockAdsToggle = document.getElementById('input-block-ads');`  
  
			`if (blockAdsToggle) blockAdsToggle.checked = true;`  
  
			`const autoRotateUserProxyCheck = document.getElementById('input-auto-rotate-user-proxy');`  
  
			`if (autoRotateUserProxyCheck) autoRotateUserProxyCheck.checked = false;`  
  
            `const userProxyToggle = document.getElementById('user-proxy-mode-toggle');`  
  
            `if (userProxyToggle) userProxyToggle.checked = false;`  
  
            `if (typeof window.toggleUserProxyMode === 'function') window.toggleUserProxyMode(false);`  
  
            `const userLocSelect = document.getElementById('user-location-select');`  
  
            `if (userLocSelect) userLocSelect.value = '';`  
  
            `const userLocSearch = document.getElementById('user-location-search');`  
  
            `if (userLocSearch) {`  
  
                `userLocSearch.value = '';`  
  
                `if (typeof window.filterUserLocations === 'function') window.filterUserLocations();`  
  
            `}`  
  
            `const userSocksInput = document.getElementById('user-socks5-input');`  
  
            `if (userSocksInput) userSocksInput.value = '';`  
  
            `const userProxyResult = document.getElementById('test-user-proxy-result');`  
  
            `if (userProxyResult) userProxyResult.innerText = '';`  
  
			`document.getElementById('hidden-auto-rotate').value = '0';`  
  
			`document.getElementById('hidden-rotate-time').value = '';`  
  
			`document.getElementById('hidden-ip-operator').value = 'all';`  
  
			`document.getElementById('hidden-ip-count').value = '20';`  
  
            `toggleModal(true);`  
  
        `}`  
  
        `const themeToggleBtn = document.getElementById('theme-toggle');`  
  
		`if (localStorage.getItem('color-theme') === 'light') {`  
  
    		`document.documentElement.classList.remove('dark');`  
  
		`} else {`  
  
    		`document.documentElement.classList.add('dark');`  
  
		`}`  
  
        `themeToggleBtn.addEventListener('click', () =&gt; {`  
  
            `if (document.documentElement.classList.contains('dark')) {`  
  
                `document.documentElement.classList.remove('dark');`  
  
                `localStorage.setItem('color-theme', 'light');`  
  
            `} else {`  
  
                `document.documentElement.classList.add('dark');`  
  
                `localStorage.setItem('color-theme', 'dark');`  
  
            `}`  
  
        `});`  
  
		`async function handleCoreAction(actionType, token = null) {`  
  
			`window.pendingCoreAction = actionType;`  
  
			`const isUpdate = actionType === 'update';`  
  
			`if (!isUpdate &amp;&amp; !await customConfirm('آیا از ری استارت پـنـل مطمئن هستید؟ کاربران شما لحظه ای قطع خواهند شد.')) return;`  
  
			`if (isUpdate &amp;&amp; !token) toggleUpdateModal(false);`  
  
			`const btn = isUpdate ? document.getElementById('update-toggle') : document.querySelector('button[title="ری استارت پـنـل"]');`  
  
			`if (btn) {`  
  
				`btn.disabled = true;`  
  
				`if (!isUpdate) btn.classList.add('animate-pulse');`  
  
			`}`  
  
			`if (isUpdate &amp;&amp; !token) alert('در حال دریافت و اعمال آپدیت... لطفاً چند ثانیه صبر کنید.');`  
  
			`try {`  
  
				`const reqBody = token ? JSON.stringify({ cf_token: token }) : "{}";`  
  
				`const res = await fetch(isUpdate ? '/api/update-panel' : '/api/restart-core', {`  
  
					`method: 'POST',`  
  
					`headers: { 'Content-Type': 'application/json' },`  
  
					`body: isUpdate ? reqBody : undefined`  
  
				`});`  
  
				`const data = await res.json();`  
  
				`if (res.status === 400 &amp;&amp; data.error === "TOKEN_REQUIRED") {`  
  
					`toggleTokenModal(true);`  
  
					`if (btn) {`  
  
						`btn.disabled = false;`  
  
						`if (!isUpdate) btn.classList.remove('animate-pulse');`  
  
					`}`  
  
					`return;`  
  
				`}`  
  
				`if (res.ok &amp;&amp; data.success) {`  
  
					`if (isUpdate) {`  
  
						`const successModal = document.getElementById('update-success-modal');`  
  
						`const successCard = successModal.querySelector('div');`  
  
						`successModal.classList.remove('opacity-0', 'pointer-events-none');`  
  
						`successModal.classList.add('opacity-100', 'pointer-events-auto');`  
  
						`successCard.classList.remove('opacity-0', 'scale-95');`  
  
						`successCard.classList.add('opacity-100', 'scale-100');`  
  
						`setTimeout(() =&gt; window.location.reload(), 10000);`  
  
					`} else {`  
  
						`alert('پـنـل ری استارت شد صفحه رفرش می شود.');`  
  
						`window.location.reload();`  
  
					`}`  
  
				`} else {`  
  
					`alert(isUpdate ? 'خطا در بروزرسانی. لطفاً با استفاده از " ربات" اقدام کنید.' : 'خطا در ری‌استارت پـنـل: ' + (data.error || 'ناشناخته'));`  
  
					`if (btn) {`  
  
						`btn.disabled = false;`  
  
						`if (!isUpdate) btn.classList.remove('animate-pulse');`  
  
					`}`  
  
				`}`  
  
			`} catch (err) {`  
  
				`alert(isUpdate ? 'خطا در ارتباط با سرور. لطفاً از گزینه آپدیت دستی استفاده کنید.' : 'خطا در ارتباط با سرور.');`  
  
				`if (btn) {`  
  
					`btn.disabled = false;`  
  
					`if (!isUpdate) btn.classList.remove('animate-pulse');`  
  
				`}`  
  
			`}`  
  
		`}`  
  
		`async function restartCore() {`  
  
			`await handleCoreAction('restart');`  
  
		`}`  
  
        `async function loadUsers(silent = false) {`  
  
            `const loadingState = document.getElementById('loading-state');`  
  
            `const tableContainer = document.getElementById('users-table-container');`  
  
            `const emptyState = document.getElementById('empty-state');`  
  
            `if (!silent) {`  
  
                `loadingState.classList.remove('hidden');`  
  
                `tableContainer.classList.add('hidden');`  
  
                `emptyState.classList.add('hidden');`  
  
            `}`  
  
            `try {`  
  
                `const res = await fetch('/api/users?t=' + [Date.now](http://Date.now)());`  
  
                `if (!res.ok) throw new Error();`  
  
                `const data = await res.json();`  
  
                `renderUsersUI(data);`  
  
            `} catch (err) {`  
  
                `if (!silent) {`  
  
                    `loadingState.innerHTML = '&lt;span class="text-red-500"&gt;خطا در دریافت اطلاعات از سرور&lt;/span&gt;';`  
  
                `}`  
  
            `}`  
  
        `}`  
  
        `function renderUsersUI(data) {`  
  
            `try {`  
  
                `const users = data.users || [];`  
  
                `window.allUsers = users;`  
  
                `const serverTime = data.serverTime || [Date.now](http://Date.now)();`  
  
                `window.lastServerTime = serverTime;`  
  
                `const totalUsersCount = users.length;`  
  
                `const activeUsersCount = users.reduce((sum, u) =&gt; sum + ([u.online](http://u.online)_count || 0), 0);`  
  
                `const totalGbUsage = users.reduce((sum, u) =&gt; sum + (u.lifetime_used_gb || u.used_gb || 0), 0);`  
  
                `document.getElementById('stat-total-users').innerText = totalUsersCount;`  
  
                `document.getElementById('stat-active-users').innerText = activeUsersCount;`  
  
                `document.getElementById('stat-total-usage').innerText = totalGbUsage &lt; 1 ? (totalGbUsage  1024).toFixed(0) + ' MB' : totalGbUsage.toFixed(2) + ' GB';`  
  
                `const cfRequests = data.cfRequestsToday || 0;`  
  
                `const reqCard = document.getElementById('card-cf-requests');`  
  
                `const warningBtn = document.getElementById('cf-warning-btn');`  
  
                `if (cfRequests &gt;= 90000) {`  
  
					`if (reqCard) {`  
  
						`reqCard.className = "bg-red-50 dark:bg-red-950/20 border border-red-500 rounded-md p-2.5 shadow-[0_0_15px_rgba(239,68,68,0.4)] flex flex-col justify-center gap-1 hover:shadow-md transition duration-300 relative overflow-hidden group min-h-[64px] animate-pulse";`  
  
					`}`  
  
					`if (warningBtn) {`  
  
						`warningBtn.classList.remove('hidden');`  
  
					`}`  
  
					`if (!window.hasShownUsageWarning) {`  
  
						`openUsageWarning();`  
  
						`window.hasShownUsageWarning = true;`  
  
					`}`  
  
				`} else {`  
  
                    `if (reqCard) {`  
  
                        `reqCard.className = "bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-md p-2.5 shadow-sm flex flex-col justify-center gap-1 hover:shadow-md hover:border-orange-400 dark:hover:border-orange-500/50 transition duration-300 relative overflow-hidden group min-h-[64px]";`  
  
                    `}`  
  
                    `if (warningBtn) {`  
  
                        `warningBtn.classList.add('hidden');`  
  
                    `}`  
  
                `}`  
  
                `const cfTotal = data.cfRequestsTotal || 0;`  
  
                `document.getElementById('stat-cf-requests').innerText = cfRequests &gt;= 1000 ? (cfRequests / 1000).toFixed(1) + 'k' : cfRequests;`  
  
                `document.getElementById('stat-cf-total').innerText = cfTotal &gt;= 1000000 ? (cfTotal / 1000000).toFixed(2) + 'M' : (cfTotal &gt;= 1000 ? (cfTotal / 1000).toFixed(1) + 'k' : cfTotal);`  
  
                `const progressPercent = Math.min((cfRequests / 100000)  100, 100);`  
  
                `document.getElementById('stat-cf-progress').style.width = progressPercent + '%';`  
  
                `filterAndRenderUsers();`  
  
            `} catch (err) {`  
  
                `document.getElementById('loading-state').innerHTML = '&lt;span class="text-red-500"&gt;خطا در پردازش اطلاعات کاربران&lt;/span&gt;';`  
  
            `}`  
  
        `}`  
  
        `function filterAndRenderUsers() {`  
  
            `if (!window.allUsers) return;`  
  
            `const searchQuery = (document.getElementById('search-input').value || '').toLowerCase().trim();`  
  
            `const filterStatus = document.getElementById('filter-status').value;`  
  
            `const sortVal = document.getElementById('sort-users').value;`  
  
            `const serverTime = window.lastServerTime || [Date.now](http://Date.now)();`  
  
            `let filtered = [...window.allUsers];`  
  
            `if (searchQuery) {`  
  
                `filtered = filtered.filter(u =&gt; `  
  
                    `(u.username || '').toLowerCase().includes(searchQuery) || `  
  
                    `(u.uuid || '').toLowerCase().includes(searchQuery)`  
  
                `);`  
  
            `}`  
  
            `if (filterStatus !== 'all') {`  
  
                `filtered = filtered.filter(u =&gt; {`  
  
                    `const isOnline = [u.is](http://u.is)_online === 1;`  
  
                    `const isActive = [u.is](http://u.is)_active === 1;`  
  
                    `let isExpired = false;`  
  
                    `if (u.limit_gb &amp;&amp; u.used_gb &gt;= u.limit_gb) isExpired = true;`  
  
                    `if (u.expiry_days &amp;&amp; u.created_at) {`  
  
                        `const created = new Date(u.created_at);`  
  
                        `const expiryDate = new Date(created.getTime() + (u.expiry_days  24  60  60  1000));`  
  
                        `if (new Date(serverTime) &gt; expiryDate) isExpired = true;`  
  
                    `}`  
  
                    `if (filterStatus === 'active') return isActive &amp;&amp; !isExpired;`  
  
                    `if (filterStatus === 'inactive') return !isActive;`  
  
                    `if (filterStatus === 'online') return isOnline;`  
  
                    `if (filterStatus === 'offline') return !isOnline;`  
  
                    `if (filterStatus === 'expired') return isExpired || !isActive;`  
  
                    `return true;`  
  
                `});`  
  
            `}`  
  
            `filtered.sort((a, b) =&gt; {`  
  
                `if (sortVal === 'newest') {`  
  
                    `return [b.id](http://b.id) - [a.id](http://a.id);`  
  
                `}`  
  
                `if (sortVal === 'name') {`  
  
                    `return (a.username || '').localeCompare(b.username || '');`  
  
                `}`  
  
                `if (sortVal === 'usage-desc') {`  
  
                    `return (b.used_gb || 0) - (a.used_gb || 0);`  
  
                `}`  
  
                `if (sortVal === 'usage-asc') {`  
  
                    `return (a.used_gb || 0) - (b.used_gb || 0);`  
  
                `}`  
  
                `if (sortVal === 'expiry-asc') {`  
  
                    `const getRemaining = (u) =&gt; {`  
  
                        `if (!u.expiry_days) return Infinity;`  
  
                        `if (!u.created_at) return Infinity;`  
  
                        `const created = new Date(u.created_at);`  
  
                        `const expiryDate = new Date(created.getTime() + (u.expiry_days  24  60  60  1000));`  
  
                        `return expiryDate - new Date(serverTime);`  
  
                    `};`  
  
                    `return getRemaining(a) - getRemaining(b);`  
  
                `}`  
  
                `return 0;`  
  
            `});`  
  
            `renderFilteredUsers(filtered, serverTime);`  
  
        `}`  
  
		`function renderFilteredUsers(users, serverTime) {`  
  
            `const loadingState = document.getElementById('loading-state');`  
  
            `const tableContainer = document.getElementById('users-table-container');`  
  
            `const emptyState = document.getElementById('empty-state');`  
  
            `const tbody = document.getElementById('users-tbody');`  
  
            `let locationsMap = {};`  
  
            `try {`  
  
                `const cachedLocations = localStorage.getItem('cached_locations_list');`  
  
                `if (cachedLocations) {`  
  
                    `JSON.parse(cachedLocations).forEach(loc =&gt; {`  
  
                        `if (loc.iata &amp;&amp; loc.cca2) locationsMap[loc.iata.toUpperCase()] = loc.cca2;`  
  
                    `});`  
  
                `}`  
  
            `} catch(e) {}`  
  
            `if (users.length === 0) {`  
  
                `loadingState.classList.add('hidden');`  
  
                `emptyState.classList.remove('hidden');`  
  
                `tableContainer.classList.add('hidden');`  
  
                `if (window.allUsers &amp;&amp; window.allUsers.length &gt; 0) {`  
  
                    `emptyState.querySelector('p').innerText = 'کاربری با مشخصات جستجو شده یافت نشد.';`  
  
                `} else {`  
  
                    `emptyState.querySelector('p').innerText = 'کاربری وجود ندارد. برای ساخت اولین کاربر روی دکمه « + » کلیک کنید.';`  
  
                `}`  
  
            `} else {`  
  
                `loadingState.classList.add('hidden');`  
  
                `emptyState.classList.add('hidden');`  
  
                `tableContainer.classList.remove('hidden');`  
  
                `let locationsMap = {};`  
  
                `try {`  
  
                    `const cachedLocations = localStorage.getItem('cached_locations_list');`  
  
                    `if (cachedLocations) {`  
  
                        `JSON.parse(cachedLocations).forEach(loc =&gt; {`  
  
                            `if (loc.iata &amp;&amp; loc.cca2) locationsMap[loc.iata.toUpperCase()] = loc.cca2;`  
  
                        `});`  
  
                    `}`  
  
                `} catch(e) {}`  
  
                `let proxyFlagCache = {};`  
  
                `try { proxyFlagCache = JSON.parse(localStorage.getItem('proxy_flag_cache') || '{}'); } catch(e) {}`  
  
                `tbody.innerHTML = [users.map](http://users.map)(user =&gt; {`  
  
                    `let daysRemaining = 'نامحدود';`  
  
                    `let daysPercent = 100;`  
  
                    `if (user.expiry_days) {`  
  
                        `if (user.created_at) {`  
  
                            `const created = new Date(user.created_at);`  
  
                            `const expiryDate = new Date(created.getTime() + (user.expiry_days  24  60  60  1000));`  
  
                            `const diffDays = Math.ceil((expiryDate - new Date()) / (1000  60  60  24));`  
  
                            `daysRemaining = diffDays &gt; 0 ? diffDays : 0;`  
  
                            `daysPercent = Math.max(0, Math.min(100, (daysRemaining / user.expiry_days)  100));`  
  
                        `} else {`  
  
                            `daysRemaining = user.expiry_days;`  
  
                        `}`  
  
                    `}`  
  
                    `const usedGb = user.used_gb || 0;`  
  
                    `const formattedUsed = usedGb &lt; 1 ? (usedGb  1024).toFixed(0) + ' MB' : usedGb.toFixed(2) + ' GB';`  
  
					`const usedReq = user.used_req || 0;`  
  
					`let reqHtml = '';`  
  
					`if (user.limit_req) {`  
  
					    `const reqPercent = Math.min((usedReq / user.limit_req)  100, 100);`  
  
					    `const reqHue = 120 - (reqPercent  1.2);`  
  
					    `reqHtml = '&lt;div class="flex flex-col gap-1.5 w-full min-w-[65px] max-w-[90px] mx-auto select-none"&gt;' +`  
  
					        `'&lt;div class="flex flex-row items-center justify-between text-[9px] text-gray-500 dark:text-gray-400 font-medium whitespace-nowrap"&gt;' +`  
  
					            `'&lt;span class="text-gray-800 dark:text-zinc-200 leading-none font-bold" dir="ltr"&gt;' + usedReq.toLocaleString() + '&lt;/span&gt;' +`  
  
					            `'&lt;button onclick="resetUserData(\\'' + encodeURIComponent(user.username) + '\\', \\'req\\')" title="ریست" class="mx-1.5 w-3.5 h-3.5 flex items-center justify-center bg-amber-50 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/50 rounded border border-amber-200 dark:border-amber-800 transition shadow-sm cursor-pointer flex-shrink-0"&gt;&lt;svg class="w-2.5 h-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"&gt;&lt;/path&gt;&lt;/svg&gt;&lt;/button&gt;' +`  
  
					            `'&lt;span class="leading-none font-bold" dir="ltr"&gt;' + user.limit_req.toLocaleString() + '&lt;/span&gt;' +`  
  
					        `'&lt;/div&gt;' +`  
  
					        `'&lt;div class="w-full h-1.5 bg-gray-200 dark:bg-zinc-700 rounded-full overflow-hidden"&gt;' +`  
  
					            `'&lt;div class="h-full rounded-full transition-all duration-500" style="width: ' + reqPercent + '%; background-color: hsl(' + reqHue + ', 80%, 45%)"&gt;&lt;/div&gt;' +`  
  
					        `'&lt;/div&gt;' +`  
  
					    `'&lt;/div&gt;';`  
  
					`} else {`  
  
					    `reqHtml = '&lt;div class="flex flex-col gap-1.5 w-full min-w-[65px] max-w-[90px] mx-auto select-none"&gt;' +`  
  
					        `'&lt;div class="flex flex-row items-center justify-between text-[9px] text-gray-500 dark:text-gray-400 font-medium whitespace-nowrap"&gt;' +`  
  
					            `'&lt;span class="text-gray-800 dark:text-zinc-200 leading-none font-bold" dir="ltr"&gt;' + usedReq.toLocaleString() + '&lt;/span&gt;' +`  
  
					            `'&lt;button onclick="resetUserData(\\'' + encodeURIComponent(user.username) + '\\', \\'req\\')" title="ریست" class="mx-1.5 w-3.5 h-3.5 flex items-center justify-center bg-amber-50 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/50 rounded border border-amber-200 dark:border-amber-800 transition shadow-sm cursor-pointer flex-shrink-0"&gt;&lt;svg class="w-2.5 h-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"&gt;&lt;/path&gt;&lt;/svg&gt;&lt;/button&gt;' +`  
  
					            `'&lt;span class="leading-none text-[12px] font-bold"&gt;∞&lt;/span&gt;' +`  
  
					        `'&lt;/div&gt;' +`  
  
					        `'&lt;div class="w-full h-1.5 bg-gray-200 dark:bg-zinc-700 rounded-full overflow-hidden"&gt;' +`  
  
					            `'&lt;div class="w-full h-full bg-blue-500 rounded-full transition-all duration-500"&gt;&lt;/div&gt;' +`  
  
					        `'&lt;/div&gt;' +`  
  
					    `'&lt;/div&gt;';`  
  
					`}`  
  
					`let volumeHtml = '';`  
  
					`if (user.limit_gb) {`  
  
					    `const limitPercent = Math.min((usedGb / user.limit_gb)  100, 100);`  
  
					    `const limitHue = 120 - (limitPercent  1.2);`  
  
					    `const formattedLimit = user.limit_gb &lt; 1 ? (user.limit_gb  1024).toFixed(0) + 'MB' : user.limit_gb + 'GB';`  
  
					    `const formattedUsedClean = usedGb &lt; 1 ? (usedGb  1024).toFixed(0) + 'MB' : usedGb.toFixed(2) + 'GB';`  
  
					    `volumeHtml = '&lt;div class="flex flex-col gap-1.5 w-full min-w-[65px] max-w-[90px] mx-auto select-none"&gt;' +`  
  
					        `'&lt;div class="flex flex-row items-center justify-between text-[9px] text-gray-500 dark:text-gray-400 font-medium whitespace-nowrap"&gt;' +`  
  
					            `'&lt;span class="text-gray-800 dark:text-zinc-200 leading-none font-bold" dir="ltr"&gt;' + formattedUsedClean + '&lt;/span&gt;' +`  
  
					            `'&lt;button onclick="resetUserData(\\'' + encodeURIComponent(user.username) + '\\', \\'volume\\')" title="ریست" class="mx-1.5 w-3.5 h-3.5 flex items-center justify-center bg-amber-50 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/50 rounded border border-amber-200 dark:border-amber-800 transition shadow-sm cursor-pointer flex-shrink-0"&gt;&lt;svg class="w-2.5 h-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"&gt;&lt;/path&gt;&lt;/svg&gt;&lt;/button&gt;' +`  
  
					            `'&lt;span class="leading-none font-bold" dir="ltr"&gt;' + formattedLimit + '&lt;/span&gt;' +`  
  
					        `'&lt;/div&gt;' +`  
  
					        `'&lt;div class="w-full h-1.5 bg-gray-200 dark:bg-zinc-700 rounded-full overflow-hidden"&gt;' +`  
  
					            `'&lt;div class="h-full rounded-full transition-all duration-500" style="width: ' + limitPercent + '%; background-color: hsl(' + limitHue + ', 80%, 45%)"&gt;&lt;/div&gt;' +`  
  
					        `'&lt;/div&gt;' +`  
  
					    `'&lt;/div&gt;';`  
  
					`} else {`  
  
					    `const formattedUsedClean = usedGb &lt; 1 ? (usedGb  1024).toFixed(0) + 'MB' : usedGb.toFixed(2) + 'GB';`  
  
					    `volumeHtml = '&lt;div class="flex flex-col gap-1.5 w-full min-w-[65px] max-w-[90px] mx-auto select-none"&gt;' +`  
  
					        `'&lt;div class="flex flex-row items-center justify-between text-[9px] text-gray-500 dark:text-gray-400 font-medium whitespace-nowrap"&gt;' +`  
  
					            `'&lt;span class="text-gray-800 dark:text-zinc-200 leading-none font-bold" dir="ltr"&gt;' + formattedUsedClean + '&lt;/span&gt;' +`  
  
					            `'&lt;button onclick="resetUserData(\\'' + encodeURIComponent(user.username) + '\\', \\'volume\\')" title="ریست" class="mx-1.5 w-3.5 h-3.5 flex items-center justify-center bg-amber-50 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/50 rounded border border-amber-200 dark:border-amber-800 transition shadow-sm cursor-pointer flex-shrink-0"&gt;&lt;svg class="w-2.5 h-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"&gt;&lt;/path&gt;&lt;/svg&gt;&lt;/button&gt;' +`  
  
					            `'&lt;span class="leading-none text-[12px] font-bold"&gt;∞&lt;/span&gt;' +`  
  
					        `'&lt;/div&gt;' +`  
  
					        `'&lt;div class="w-full h-1.5 bg-gray-200 dark:bg-zinc-700 rounded-full overflow-hidden"&gt;' +`  
  
					            `'&lt;div class="w-full h-full bg-blue-500 rounded-full transition-all duration-500"&gt;&lt;/div&gt;' +`  
  
					        `'&lt;/div&gt;' +`  
  
					    `'&lt;/div&gt;';`  
  
					`}`  
  
					`let expiryHtml = '';`  
  
					`if (user.expiry_days) {`  
  
					    `const expiryHue = daysPercent  1.2;`  
  
					    `expiryHtml = '&lt;div class="flex flex-col gap-1.5 w-full min-w-[65px] max-w-[90px] mx-auto select-none"&gt;' +`  
  
					        `'&lt;div class="flex flex-row items-center justify-between text-[9px] text-gray-500 dark:text-gray-400 font-medium whitespace-nowrap"&gt;' +`  
  
					            `'&lt;span class="text-gray-800 dark:text-zinc-200 leading-none font-bold" dir="rtl"&gt;' + daysRemaining + ' روز&lt;/span&gt;' +`  
  
					            `'&lt;button onclick="resetUserData(\\'' + encodeURIComponent(user.username) + '\\', \\'time\\')" title="ریست" class="mx-1.5 w-3.5 h-3.5 flex items-center justify-center bg-amber-50 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/50 rounded border border-amber-200 dark:border-amber-800 transition shadow-sm cursor-pointer flex-shrink-0"&gt;&lt;svg class="w-2.5 h-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"&gt;&lt;/path&gt;&lt;/svg&gt;&lt;/button&gt;' +`  
  
					            `'&lt;span class="leading-none font-bold" dir="rtl"&gt;' + user.expiry_days + ' روز&lt;/span&gt;' +`  
  
					        `'&lt;/div&gt;' +`  
  
					        `'&lt;div class="w-full h-1.5 bg-gray-200 dark:bg-zinc-700 rounded-full overflow-hidden flex justify-end"&gt;' +`  
  
					            `'&lt;div class="h-full rounded-full transition-all duration-500" style="width: ' + daysPercent + '%; background-color: hsl(' + expiryHue + ', 80%, 45%)"&gt;&lt;/div&gt;' +`  
  
					        `'&lt;/div&gt;' +`  
  
					    `'&lt;/div&gt;';`  
  
					`} else {`  
  
					    `expiryHtml = '&lt;div class="flex flex-col gap-1.5 w-full min-w-[65px] max-w-[90px] mx-auto select-none"&gt;' +`  
  
					        `'&lt;div class="flex flex-row items-center justify-between text-[9px] text-gray-500 dark:text-gray-400 font-medium whitespace-nowrap"&gt;' +`  
  
					            `'&lt;span class="text-gray-800 dark:text-zinc-200 leading-none font-bold text-[12px]"&gt;∞&lt;/span&gt;' +`  
  
					            `'&lt;button onclick="resetUserData(\\'' + encodeURIComponent(user.username) + '\\', \\'time\\')" title="ریست" class="mx-1.5 w-3.5 h-3.5 flex items-center justify-center bg-amber-50 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/50 rounded border border-amber-200 dark:border-amber-800 transition shadow-sm cursor-pointer flex-shrink-0"&gt;&lt;svg class="w-2.5 h-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"&gt;&lt;/path&gt;&lt;/svg&gt;&lt;/button&gt;' +`  
  
					            `'&lt;span class="leading-none text-[12px] font-bold"&gt;∞&lt;/span&gt;' +`  
  
					        `'&lt;/div&gt;' +`  
  
					        `'&lt;div class="w-full h-1.5 bg-gray-200 dark:bg-zinc-700 rounded-full overflow-hidden"&gt;' +`  
  
					            `'&lt;div class="w-full h-full bg-blue-500 rounded-full transition-all duration-500"&gt;&lt;/div&gt;' +`  
  
					        `'&lt;/div&gt;' +`  
  
					    `'&lt;/div&gt;';`  
  
					`}`  
  
                    `const onlineCount = [user.online](http://user.online)_count || 0;`  
  
                    `const limit = user.ip_limit !== undefined ? user.ip_limit : user.max_connections;`  
  
                    `let onlineHtml = '';`  
  
                    `if (limit) {`  
  
                        `const onlinePercent = Math.min((onlineCount / limit)  100, 100);`  
  
                        `const onlineHue = 120 - (onlinePercent  1.2);`  
  
                        `onlineHtml = '&lt;div class="flex flex-col gap-1.5 w-full min-w-[65px] max-w-[90px] mx-auto select-none"&gt;' +`  
  
                            `'&lt;div class="flex flex-row items-center justify-between text-[9px] text-gray-500 dark:text-gray-400 font-medium whitespace-nowrap"&gt;' +`  
  
                                `'&lt;span class="text-gray-800 dark:text-zinc-200 leading-none font-bold" dir="ltr"&gt;' + onlineCount + '&lt;/span&gt;' +`  
  
                                `'&lt;span class="leading-none font-bold" dir="ltr"&gt;' + limit + '&lt;/span&gt;' +`  
  
                            `'&lt;/div&gt;' +`  
  
                            `'&lt;div class="w-full h-1.5 bg-gray-200 dark:bg-zinc-700 rounded-full overflow-hidden"&gt;' +`  
  
                                `'&lt;div class="h-full rounded-full transition-all duration-500" style="width: ' + onlinePercent + '%; background-color: hsl(' + onlineHue + ', 80%, 45%)"&gt;&lt;/div&gt;' +`  
  
                            `'&lt;/div&gt;' +`  
  
                        `'&lt;/div&gt;';`  
  
                    `} else {`  
  
                        `onlineHtml = '&lt;div class="flex flex-col gap-1.5 w-full min-w-[65px] max-w-[90px] mx-auto select-none"&gt;' +`  
  
                            `'&lt;div class="flex flex-row items-center justify-between text-[9px] text-gray-500 dark:text-gray-400 font-medium whitespace-nowrap"&gt;' +`  
  
                                `'&lt;span class="text-gray-800 dark:text-zinc-200 leading-none font-bold" dir="ltr"&gt;' + onlineCount + '&lt;/span&gt;' +`  
  
                                `'&lt;span class="leading-none text-[12px] font-bold"&gt;∞&lt;/span&gt;' +`  
  
                            `'&lt;/div&gt;' +`  
  
                            `'&lt;div class="w-full h-1.5 bg-gray-200 dark:bg-zinc-700 rounded-full overflow-hidden"&gt;' +`  
  
                                `'&lt;div class="h-full ' + (onlineCount &gt; 0 ? 'bg-green-600' : 'bg-gray-400') + ' rounded-full transition-all duration-500" style="width: 100%"&gt;&lt;/div&gt;' +`  
  
                            `'&lt;/div&gt;' +`  
  
                        `'&lt;/div&gt;';`  
  
                    `}`  
  
                    `let isExpired = false;`  
  
                    `if (user.limit_gb &amp;&amp; (user.used_gb || 0) &gt;= user.limit_gb) isExpired = true;`  
  
                    `if (user.limit_req &amp;&amp; (user.used_req || 0) &gt;= user.limit_req) isExpired = true;`  
  
                    `if (user.expiry_days &amp;&amp; user.created_at) {`  
  
                        `const created = new Date(user.created_at);`  
  
                        `const expiryDate = new Date(created.getTime() + (user.expiry_days  24*  60  60  1000));`  
  
                        `if (new Date(serverTime) &gt; expiryDate) isExpired = true;`  
  
                    `}`  
  
                    `const isEffectivelyActive = [user.is](http://user.is)_active !== 0 &amp;&amp; !isExpired;`  
  
                    `const statusBtnColor = [user.is](http://user.is)_active === 0 ? 'text-green-700 dark:text-green-500 hover:bg-green-50 dark:hover:bg-green-900/30' : 'text-amber-600 dark:text-amber-400 hover:bg-amber-50 dark:hover:bg-amber-900/30';`  
  
                    `const statusBtnTitle = [user.is](http://user.is)_active === 0 ? 'فعال کردن کاربر' : 'قطع کردن کاربر';`  
  
                    `const statusBtnIcon = [user.is](http://user.is)_active === 0 `  
  
                        `? '&lt;svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"&gt;&lt;/path&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"&gt;&lt;/path&gt;&lt;/svg&gt;'`  
  
                        `: '&lt;svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z"&gt;&lt;/path&gt;&lt;/svg&gt;';`  
  
                    `const isChecked = (window.selectedUsernames &amp;&amp; window.selectedUsernames.has(user.username)) ? 'checked' : '';`  
  
                    `let locBadge = '';`  
  
                    `if (user.user_proxy_iata) {`  
  
                        `const iata = user.user_proxy_iata.toUpperCase();`  
  
                        `const cca2 = locationsMap[iata];`  
  
                        `const flag = cca2 ? getFlagEmoji(cca2) : '🌐';`  
  
                        `locBadge = '&lt;span title="کشور: ' + iata + '" class="text-base leading-none px-0.5 drop-shadow-[0_0_2px_rgba(0,0,0,0.3)] dark:drop-shadow-[0_0_2px_rgba(255,255,255,0.3)]"&gt;' + flag + '&lt;/span&gt;';`  
  
                    `} else if (user.user_socks5 || user.user_proxy_ip) {`  
  
                        `const targetProxy = user.user_socks5 || user.user_proxy_ip;`  
  
                        `const cachedFlag = proxyFlagCache[targetProxy];`  
  
                        `if (cachedFlag) {`  
  
                            `locBadge = '&lt;span title="پـروکـسـی اختصاصی" class="text-base leading-none px-0.5 drop-shadow-[0_0_2px_rgba(0,0,0,0.3)] dark:drop-shadow-[0_0_2px_rgba(255,255,255,0.3)]"&gt;' + cachedFlag + '&lt;/span&gt;';`  
  
                        `} else {`  
  
                            `locBadge = '&lt;span data-proxy="' + targetProxy + '" title="پـروکـسـی اختصاصی" class="async-proxy-flag text-base leading-none px-0.5 drop-shadow-[0_0_2px_rgba(0,0,0,0.3)] dark:drop-shadow-[0_0_2px_rgba(255,255,255,0.3)]"&gt;⏳&lt;/span&gt;';`  
  
                        `}`  
  
                    `}`  
  
                    `return '&lt;tr class="hover:bg-gray-50 dark:hover:bg-zinc-900/40 border-b border-gray-100 dark:border-zinc-800 last:border-0"&gt;' +`  
  
                            `'&lt;td class="p-1 border-r border-gray-100 dark:border-zinc-800 text-center select-none"&gt;' +`  
  
                                `'&lt;input type="checkbox" name="select-user" value="' + encodeURIComponent(user.username) + '" onchange="onUserSelectChange(this)" ' + isChecked + ' class="w-4 h-4 rounded-md border-2 border-gray-300 dark:border-zinc-700 text-blue-600 bg-white dark:bg-zinc-800 checked:bg-blue-600 checked:border-blue-600 focus:ring-blue-500/50 focus:ring-offset-0 transition-all duration-200 cursor-pointer hover:scale-105 active:scale-95"&gt;' +`  
  
                            `'&lt;/td&gt;' +`  
  
                            `'&lt;td class="p-1 border-r border-gray-100 dark:border-zinc-800 text-center"&gt;' +`  
  
                                `'&lt;div class="flex flex-col items-center justify-center gap-1 w-full max-w-[120px] mx-auto select-none"&gt;' +`  
  
                                    `'&lt;span class="font-bold text-gray-900 dark:text-zinc-100 text-xs truncate max-w-full pb-0.5"&gt;' + user.username + '&lt;/span&gt;' +`  
  
                                    `'&lt;div class="flex flex-row items-center justify-center gap-1 whitespace-nowrap"&gt;' +`  
  
                                        `(!isEffectivelyActive ? '&lt;span class="px-1 py-px text-[9px] font-medium bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400 rounded"&gt;غیرفعال&lt;/span&gt;' : '&lt;span class="px-1 py-px text-[9px] font-medium bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400 rounded"&gt;فعال&lt;/span&gt;') +`  
  
                                        `locBadge +`  
  
                                        `([user.is](http://user.is)_online === 1 ? '&lt;span class="px-1 py-px text-[9px] font-medium bg-green-600 text-white rounded animate-pulse" dir="rtl"&gt;' + [user.online](http://user.online)_count + '&lt;/span&gt;' : '&lt;span class="px-1 py-px text-[9px] font-medium bg-gray-200 text-gray-600 dark:bg-zinc-800 dark:text-zinc-400 rounded"&gt;آفلاین&lt;/span&gt;') +`  
  
                                    `'&lt;/div&gt;' +`  
  
                                `'&lt;/div&gt;' +`  
  
                            `'&lt;/td&gt;' +`  
  
                            `'&lt;td class="p-1.5 border-r border-gray-100 dark:border-zinc-800 text-center"&gt;' +`  
  
                                `'&lt;div class="grid grid-cols-2 gap-1 w-max mx-auto"&gt;' +`  
  
                                    `'&lt;button onclick="copyConfig(\\'' + encodeURIComponent(user.username) + '\\')" title="کپی کـانفـیگ" class="p-1 flex items-center justify-center bg-white dark:bg-zinc-800 border border-gray-200 dark:border-zinc-700 hover:bg-blue-50 dark:hover:bg-blue-900/30 text-blue-600 dark:text-blue-400 rounded transition shadow-sm"&gt;&lt;svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"&gt;&lt;/path&gt;&lt;/svg&gt;&lt;/button&gt;' +`  
  
                                    `'&lt;button onclick="editUser(\\'' + encodeURIComponent(user.username) + '\\')" title="ویرایش" class="p-1 flex items-center justify-center bg-white dark:bg-zinc-800 border border-gray-200 dark:border-zinc-700 hover:bg-yellow-50 dark:hover:bg-yellow-900/30 text-yellow-600 dark:text-yellow-400 rounded transition shadow-sm"&gt;&lt;svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"&gt;&lt;/path&gt;&lt;/svg&gt;&lt;/button&gt;' +`  
  
                                    `'&lt;button onclick="deleteUser(\\'' + encodeURIComponent(user.username) + '\\')" title="حذف" class="p-1 flex items-center justify-center bg-white dark:bg-zinc-800 border border-gray-200 dark:border-zinc-700 hover:bg-red-50 dark:hover:bg-red-950/20 text-red-600 dark:text-red-400 rounded transition shadow-sm"&gt;&lt;svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"&gt;&lt;/path&gt;&lt;/svg&gt;&lt;/button&gt;' +`  
  
                                    `'&lt;button onclick="toggleUserStatus(\\'' + encodeURIComponent(user.username) + '\\')" title="' + statusBtnTitle + '" class="p-1 flex items-center justify-center bg-white dark:bg-zinc-800 border border-gray-200 dark:border-zinc-700 ' + statusBtnColor + ' rounded transition shadow-sm"&gt;' + statusBtnIcon + '&lt;/button&gt;' +`  
  
                                `'&lt;/div&gt;' +`  
  
                            `'&lt;/td&gt;' +`  
  
                            `'&lt;td class="p-1 border-r border-gray-100 dark:border-zinc-800"&gt;' +`  
  
							    `'&lt;div class="flex flex-col gap-0.5 w-[90px] mx-auto"&gt;' +`  
  
							        `'&lt;button onclick="copySubLink(\\'' + encodeURIComponent(user.username) + '\\')" class="w-full flex items-center justify-center gap-1 px-1 py-0.5 bg-indigo-50 dark:bg-indigo-900/30 text-indigo-600 dark:text-indigo-400 hover:bg-indigo-100 dark:hover:bg-indigo-900/50 rounded text-[9px] font-bold transition border border-indigo-200 dark:border-indigo-800"&gt;' +`  
  
							            `'&lt;svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"&gt;&lt;/path&gt;&lt;/svg&gt;' +`  
  
							            `'ساب متنی' +`  
  
							        `'&lt;/button&gt;' +`  
  
							        `'&lt;div class="flex flex-row gap-0.5 w-full h-[22px]"&gt;' +`  
  
							            `'&lt;button onclick="copyStatusLink(\\'' + encodeURIComponent(user.username) + '\\')" class="flex-1 flex items-center justify-center gap-1 px-1 py-0 bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-500 hover:bg-green-100 dark:hover:bg-green-900/50 rounded text-[9px] font-bold transition border border-green-200 dark:border-green-800 whitespace-nowrap"&gt;' +`  
  
							                `'&lt;svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"&gt;&lt;/path&gt;&lt;/svg&gt;' +`  
  
							                `'وضعیت' +`  
  
							            `'&lt;/button&gt;' +`  
  
							            `'&lt;button onclick="showSubQr(\\'' + encodeURIComponent(user.username) + '\\')" title="QR ساب" class="w-[22px] flex-shrink-0 flex items-center justify-center bg-amber-50 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/50 rounded transition border border-amber-200 dark:border-amber-800"&gt;' +`  
  
							                `'&lt;svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm14 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 19h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z"&gt;&lt;/path&gt;&lt;/svg&gt;' +`  
  
							            `'&lt;/button&gt;' +`  
  
							        `'&lt;/div&gt;' +`  
  
							    `'&lt;/div&gt;' +`  
  
							`'&lt;/td&gt;' +`  
  
							`'&lt;td class="p-1 border-r border-gray-100 dark:border-zinc-800 text-xs"&gt;' + `  
  
							    `'&lt;div class="grid grid-flow-col grid-rows-3 gap-1 w-max mx-auto"&gt;' +`  
  
							        `String(user.port || "").split(",").map(function(p) {`  
  
							            `p = p.trim();`  
  
							            `if (!p) return "";`  
  
							            `var isTls = tlsPorts.includes(p);`  
  
							            `var isNonTls = nonTlsPorts.includes(p);`  
  
							            `var colorClass = isTls ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400' : `  
  
							                             `isNonTls ? 'bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-400' : `  
  
							                             `'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400';`  
  
							            `return '&lt;span class="inline-flex items-center justify-center px-1.5 py-0.5 text-[10px] font-semibold rounded leading-none ' + colorClass + '"&gt;' + p + '&lt;/span&gt;';`  
  
							        `}).join("") +`  
  
							    `'&lt;/div&gt;' +`  
  
							`'&lt;/td&gt;' +`  
  
							`'&lt;td class="p-1.5 border-r border-gray-100 dark:border-zinc-800"&gt;' + volumeHtml + '&lt;/td&gt;' +`  
  
							`'&lt;td class="p-1.5 border-r border-gray-100 dark:border-zinc-800"&gt;' + reqHtml + '&lt;/td&gt;' +`  
  
							`'&lt;td class="p-1.5 border-r border-gray-100 dark:border-zinc-800"&gt;' + expiryHtml + '&lt;/td&gt;' +`  
  
							`'&lt;td class="p-1.5 border-r border-gray-100 dark:border-zinc-800"&gt;' + onlineHtml + '&lt;/td&gt;' +`  
  
							`'&lt;/tr&gt;';`  
  
                `}).join('');`  
  
                `updateBulkActionsBar();`  
  
                `if (typeof loadProxyFlags === 'function') {`  
  
                    `setTimeout(loadProxyFlags, 50);`  
  
                `}`  
  
            `}`  
  
        `}`  
  
		`async function resetUserData(encodedUsername, actionType) {`  
  
			`const username = decodeURIComponent(encodedUsername);`  
  
			`let actionName = '';`  
  
			`if (actionType === 'volume') actionName = 'حجم';`  
  
			`else if (actionType === 'req') actionName = 'ریکوئست';`  
  
			`else if (actionType === 'time') actionName = 'زمان';`  
  
			`if (await customConfirm('آیا از ریست کردن ' + actionName + ' کاربر ' + username + ' مطمئن هستید؟')) {`  
  
                `try {`  
  
                    `const response = await fetch('/api/users/' + encodeURIComponent(username), {`  
  
                        `method: 'PUT',`  
  
                        `headers: { 'Content-Type': 'application/json' },`  
  
                        `body: JSON.stringify({ reset_action: actionType })`  
  
                    `});`  
  
                    `if (response.ok) {`  
  
                        `alert('عملیات با موفقیت انجام شد.');`  
  
                        `await loadUsers(true);`  
  
                    `} else {`  
  
                        `const errData = await response.json();`  
  
                        `alert('خطا: ' + (errData.error || 'عملیات ناموفق بود'));`  
  
                    `}`  
  
                `} catch (err) {`  
  
                    `alert('خطا در برقراری ارتباط با سرور');`  
  
                `}`  
  
            `}`  
  
        `}`  
  
        `async function toggleUserStatus(encodedUsername) {`  
  
            `const username = decodeURIComponent(encodedUsername);`  
  
            `try {`  
  
                `const response = await fetch('/api/users/' + encodeURIComponent(username), {`  
  
                    `method: 'PUT',`  
  
                    `headers: { 'Content-Type': 'application/json' },`  
  
                    `body: JSON.stringify({ toggle_only: true })`  
  
                `});`  
  
                `if (response.ok) {`  
  
                    `await loadUsers(true);`  
  
                `} else {`  
  
                    `const errData = await response.json();`  
  
                    `alert('خطا: ' + (errData.error || 'عملیات ناموفق بود'));`  
  
                `}`  
  
            `} catch (err) {`  
  
                `alert('خطا در برقراری ارتباط با سرور');`  
  
            `}`  
  
        `}`  
  
        `async function handleFormSubmit(event) {`  
  
            `event.preventDefault();`  
  
            `const submitButton = document.getElementById('submit-btn');`  
  
            `submitButton.disabled = true;`  
  
            `submitButton.innerText = isEditMode ? 'در حال ذخیره تغییرات...' : 'در حال ایجاد...';`  
  
            `const username = document.getElementById('input-name').value;`  
  
            `const usernameRegex = /^[a-zA-Z0-9_-]+$/;`  
  
            `if (!usernameRegex.test(username)) {`  
  
                `alert('⚠️ نام کاربری فقط می‌تواند شامل حروف انگلیسی، اعداد، خط تیره (-) و آندرلاین (_) باشد!');`  
  
                `submitButton.disabled = false;`  
  
                `submitButton.innerText = isEditMode ? 'ذخیره تغییرات' : 'ایجاد کاربر';`  
  
                `return;`  
  
            `}`  
  
            `const limit = document.getElementById('input-limit').value || null;`  
  
            `const expiry = document.getElementById('input-expiry').value || null;`  
  
            `const reqLimit = document.getElementById('input-req-limit').value || null;`  
  
            `const ipLimit = document.getElementById('input-ip-limit').value || null;`  
  
			`if (limit !== null &amp;&amp; parseFloat(limit) &lt; 0) { alert('⚠️ حجم نمی‌تواند عدد منفی باشد!'); submitButton.disabled = false; submitButton.innerText = isEditMode ? 'ذخیره تغییرات' : 'ایجاد کاربر'; return; }`  
  
			`if (expiry !== null &amp;&amp; parseInt(expiry) &lt; 0) { alert('⚠️ زمان (روز) نمی‌تواند عدد منفی باشد!'); submitButton.disabled = false; submitButton.innerText = isEditMode ? 'ذخیره تغییرات' : 'ایجاد کاربر'; return; }`  
  
			`if ((reqLimit !== null &amp;&amp; parseInt(reqLimit) &lt; 0) || (ipLimit !== null &amp;&amp; parseInt(ipLimit) &lt; 0)) { alert('⚠️ محدودیت‌ها نمی‌توانند منفی باشند!'); submitButton.disabled = false; submitButton.innerText = isEditMode ? 'ذخیره تغییرات' : 'ایجاد کاربر'; return; }`  
  
            `const autoResetToggle = document.getElementById('input-auto-reset-toggle').checked;`  
  
            `const autoResetVolDays = document.getElementById('input-auto-reset-vol').value;`  
  
            `const autoResetReqDays = document.getElementById('input-auto-reset-req').value;`  
  
            `if (autoResetToggle) {`  
  
                `const volDays = parseInt(autoResetVolDays) || 0;`  
  
                `const reqDays = parseInt(autoResetReqDays) || 0;`  
  
                `if (volDays &lt;= 0 &amp;&amp; reqDays &lt;= 0) {`  
  
                    `alert('⚠️ وقتی تیک تمدید خودکار روشن است، باید حداقل یکی از فیلدها (زمان تمدید حجم یا ریکوئست) را پر کنید!');`  
  
                    `submitButton.disabled = false;`  
  
                    `submitButton.innerText = isEditMode ? 'ذخیره تغییرات' : 'ایجاد کاربر';`  
  
                    `return;`  
  
                `}`  
  
            `}`  
  
			`const customPortsRaw = document.getElementById('input-custom-ports') ? document.getElementById('input-custom-ports').value : '';`  
  
			`const customPortsArray = customPortsRaw.replace(/ +/g, ',').split(',').map(p =&gt; p.trim()).filter(p =&gt; p.length &gt; 0);`  
  
			`const checkedPorts = Array.from(document.querySelectorAll('input[name="ports"]:checked')).map(cb =&gt; cb.value).concat(customPortsArray);`  
  
            `const block_porn = document.getElementById('input-block-porn').checked ? 1 : 0;`  
  
            `const block_ads = document.getElementById('input-block-ads').checked ? 1 : 0;`  
  
            `const isFragEnabled = document.getElementById('input-frag-toggle').checked;`  
  
            `const frag_len = isFragEnabled ? (document.getElementById('input-frag-len').value || "200-3000") : "";`  
  
            `const frag_int = isFragEnabled ? (document.getElementById('input-frag-int').value || "1-2") : "";`  
  
            `const isAutoReset = document.getElementById('input-auto-reset-toggle').checked;`  
  
            `const auto_reset_vol_days = isAutoReset ? parseInt(document.getElementById('input-auto-reset-vol').value) || 0 : 0;`  
  
            `const auto_reset_req_days = isAutoReset ? parseInt(document.getElementById('input-auto-reset-req').value) || 0 : 0;`  
  
            `const auto_rotate_ip = parseInt(document.getElementById('hidden-auto-rotate').value) || 0;`  
  
            `const rotate_time = parseInt(document.getElementById('hidden-rotate-time').value) || 0;`  
  
            `const ip_operator = document.getElementById('hidden-ip-operator').value || 'all';`  
  
            `const ip_count = parseInt(document.getElementById('hidden-ip-count').value) || 20;`  
  
            `const userProxyMode = document.getElementById('user-proxy-mode-toggle') ? document.getElementById('user-proxy-mode-toggle').checked : false;`  
  
            `const userLocVal = document.getElementById('user-location-select') ? document.getElementById('user-location-select').value : null;`  
  
            `const userProxyIata = (!userProxyMode &amp;&amp; userLocVal !== "") ? userLocVal : null;`  
  
            `const userSocksVal = document.getElementById('user-socks5-input') ? document.getElementById('user-socks5-input').value.trim() : null;`  
  
            `const userSocks5 = (userProxyMode &amp;&amp; userSocksVal !== "") ? userSocksVal : null;`  
  
            `const auto_rotate_user_proxy = document.getElementById('input-auto-rotate-user-proxy') ? (document.getElementById('input-auto-rotate-user-proxy').checked ? 1 : 0) : 0;`  
  
            `if (checkedPorts.length === 0) {`  
  
                `alert('⚠️ لطفا حداقل یک پورت را برای اتصال انتخاب کنید!');`  
  
                `submitButton.disabled = false;`  
  
                `submitButton.innerText = isEditMode ? 'ذخیره تغییرات' : 'ایجاد کاربر';`  
  
                `return;`  
  
            `}`  
  
            `const port = checkedPorts.join(',');`  
  
            `const tls = checkedPorts.some(p =&gt; tlsPorts.includes(p)) ? 'on' : 'off';`  
  
            `const ips = document.getElementById('input-ips').value;`  
  
            `const fingerprint = document.getElementById('fingerprint-select').value;`  
  
            `const url = isEditMode ? '/api/users/' + encodeURIComponent(editingUsername) : '/api/users';`  
  
            `const method = isEditMode ? 'PUT' : 'POST';`  
  
            `try {`  
  
                `const response = await fetch(url, {`  
  
                    `method: method,`  
  
                    `headers: { 'Content-Type': 'application/json' },`  
  
                    `body: JSON.stringify({ `  
  
                        `username, limit_gb: limit, expiry_days: expiry, limit_req: reqLimit, tls, port, ips, fingerprint, ip_limit: ipLimit, block_porn: block_porn, block_ads: block_ads, frag_len: frag_len, frag_int: frag_int,`  
  
                        `user_proxy_iata: userProxyIata || null,`  
  
                        `user_socks5: userSocks5 || null,`  
  
                        `user_proxy_ip: null,`  
  
                        `auto_reset_vol_days: auto_reset_vol_days,`  
  
                        `auto_reset_req_days: auto_reset_req_days,`  
  
                        `auto_rotate_ip: auto_rotate_ip,`  
  
                        `rotate_time: rotate_time,`  
  
                        `ip_operator: ip_operator,`  
  
                        `ip_count: ip_count,`  
  
                        `auto_rotate_user_proxy: auto_rotate_user_proxy`  
  
                    `})`  
  
                `});`  
  
                `if (response.ok) {`  
  
                    `toggleModal(false);`  
  
                    `await loadUsers(true);`  
  
                `} else {`  
  
                    `const errData = await response.json();`  
  
                    `alert('خطا: ' + (errData.error || 'عملیات ناموفق بود'));`  
  
                `}`  
  
            `} catch (err) {`  
  
                `alert('خطا در برقراری ارتباط با سرور');`  
  
            `} finally {`  
  
                `submitButton.disabled = false;`  
  
                `submitButton.innerText = isEditMode ? 'ذخیره تغییرات' : 'ایجاد کاربر';`  
  
            `}`  
  
        `}`  
  
`function setModalState(modalId, show) {`  
  
			`const modal = document.getElementById(modalId);`  
  
			`if (!modal) return;`  
  
			`const card = modal.querySelector('div');`  
  
			`if (show) {`  
  
				`modal.classList.remove('opacity-0', 'pointer-events-none');`  
  
				`modal.classList.add('opacity-100', 'pointer-events-auto');`  
  
				`card.classList.remove('opacity-0', 'scale-95');`  
  
				`card.classList.add('opacity-100', 'scale-100');`  
  
			`} else {`  
  
				`modal.classList.remove('opacity-100', 'pointer-events-auto');`  
  
				`modal.classList.add('opacity-0', 'pointer-events-none');`  
  
				`card.classList.remove('opacity-100', 'scale-100');`  
  
				`card.classList.add('opacity-0', 'scale-95');`  
  
			`}`  
  
		`}`  
  
		`function closeUsageWarning() { setModalState('usage-warning-modal', false); }`  
  
		`function openUsageWarning() { setModalState('usage-warning-modal', true); }`  
  
		`function closeFreePanelWarning() { setModalState('free-panel-warning-modal', false); }`  
  
	`async function checkGlobalMessage() {`  
  
        `try {`  
  
            `const res = await fetch('[https://zeus-files.surge.sh/message.txt?t=](https://zeus-files.surge.sh/message.txt?t=)' + [Date.now](http://Date.now)());`  
  
            `if (!res.ok) return;`  
  
            `const text = await res.text();`  
  
            `const lines = text.split('\\n');`  
  
            `if (lines.length &lt; 2) return;`  
  
            `const firstLine = lines[0].trim();`  
  
            `if (!firstLine.startsWith('VERSION=')) return;`  
  
            `const version = firstLine.split('=')[1].trim();`  
  
            `const content = lines.slice(1).join('\\n').trim();`  
  
            `if (window.zeus_global_msg_version !== version) {`  
  
                `document.getElementById('global-message-content').innerHTML = content;`  
  
                `setModalState('global-message-modal', true);`  
  
                `document.getElementById('global-message-close-btn').onclick = function() {`  
  
                    `setModalState('global-message-modal', false);`  
  
                    `window.zeus_global_msg_version = version;`  
  
                `};`  
  
            `}`  
  
        `} catch (err) {}`  
  
    `}`  
  
		`function getvIeesLink(username) {`  
  
            `const user = window.allUsers.find(u =&gt; u.username === username);`  
  
            `if (!user) return '';`  
  
            `const host = window.location.hostname;`  
  
            `var ips = [host];`  
  
            `if (user.ips) {`  
  
                `ips = user.ips.split('\\n').map(function(ip) { return ip.trim(); }).filter(function(ip) { return ip.length &gt; 0; });`  
  
                `if (ips.length === 0) ips = [host];`  
  
            `}`  
  
            `var ports = String(user.port || '443').split(',').map(function(p) { return p.trim(); }).filter(function(p) { return p.length &gt; 0; });`  
  
            `var fp = user.fingerprint || 'chrome';`  
  
            `const userFrag = (user.frag_len &amp;&amp; user.frag_int) ? '&amp;fragment=' + user.frag_len + ',' + user.frag_int : '';`  
  
            `const links = [];`  
  
		`const dynPath = encodeURIComponent("/stream/PANEL_ZEUS/" + (user.uuid ? user.uuid.split("-")[0] : "default"));`  
  
		`const m1 = decodeURIComponent('%E2%9A%A0%EF%B8%8F%D8%A7%DB%8C%D9%86%20%D9%BE%D9%86%D9%84%20%D8%B1%D8%A7%DB%8C%DA%AF%D8%A7%D9%86%20%D9%88%20%D8%BA%DB%8C%D8%B1%20%D9%82%D8%A7%D8%A8%D9%84%20%D9%81%D8%B1%D9%88%D8%B4%20%D8%A7%D8%B3%D8%AA%E2%9A%A0%EF%B8%8F');`  
  
		`const m2 = decodeURIComponent('%E2%99%A8%EF%B8%8F%20%40PANEL_ZEUS%20%D8%B3%D8%A7%D8%AE%D8%AA%20%D8%B1%D8%A7%DB%8C%DA%AF%D8%A7%D9%86%20%E2%99%A8%EF%B8%8F');`  
  
		`links.push('vle' + 'ss://' + (user.uuid || '') + '@0.0.0.0:1?encryption=none&amp;security=none&amp;type=ws&amp;host=' + host + '&amp;path=' + dynPath + '#' + encodeURIComponent(m1));`  
  
		`links.push('vle' + 'ss://' + (user.uuid || '') + '@0.0.0.0:1?encryption=none&amp;security=none&amp;type=ws&amp;host=' + host + '&amp;path=' + dynPath + '#' + encodeURIComponent(m2));`  
  
            `let flagEmoji = '🌐';`  
  
            `if (user.user_proxy_iata) {`  
  
                `try {`  
  
                    `const cachedLocations = localStorage.getItem('cached_locations_list');`  
  
                    `if (cachedLocations) {`  
  
                        `const parsedLocs = JSON.parse(cachedLocations);`  
  
                        `const loc = parsedLocs.find(l =&gt; l.iata &amp;&amp; l.iata.toUpperCase() === user.user_proxy_iata.toUpperCase());`  
  
                        `if (loc &amp;&amp; loc.cca2) flagEmoji = getFlagEmoji(loc.cca2);`  
  
                    `}`  
  
                `} catch(e) {}`  
  
            `} else if (user.user_socks5 || user.user_proxy_ip) {`  
  
                `const targetProxy = user.user_socks5 || user.user_proxy_ip;`  
  
                `try {`  
  
                    `const proxyFlagCache = JSON.parse(localStorage.getItem('proxy_flag_cache') || '{}');`  
  
                    `if (proxyFlagCache[targetProxy]) flagEmoji = proxyFlagCache[targetProxy];`  
  
                `} catch(e) {}`  
  
            `}`  
  
            `ips.forEach((ip) =&gt; {`  
  
                `ports.forEach((portStr) =&gt; {`  
  
					`const isTlsPort = tlsPorts.includes(portStr);`  
  
					`const tlsVal = isTlsPort ? 'tls' : 'none';`  
  
					`const remark = flagEmoji + ' | ' + user.username + ' | \\u200E' + ip + ' | \\u200E' + portStr;`  
  
					`links.push('vle' + 'ss://' + (user.uuid || '') + '@' + ip + ':' + portStr + '?path=' + dynPath + '&amp;security=' + tlsVal + '&amp;encryption=none&amp;insecure=0&amp;host=' + host + '&amp;fp=' + fp + '&amp;type=ws&amp;allowInsecure=0&amp;sni=' + host + userFrag + '#' + encodeURIComponent(remark));`  
  
				`});`  
  
            `});`  
  
            `return links.join('\\n');`  
  
        `}`  
  
        `function getSubLink(username) {`  
  
            `return window.location.origin + '/feed/' + encodeURIComponent(username);`  
  
        `}`  
  
        `function getStatusLink(username) {`  
  
            `return window.location.origin + '/status/' + encodeURIComponent(username);`  
  
        `}`  
  
        `function copySubLink(encodedUsername) {`  
  
            `const username = decodeURIComponent(encodedUsername);`  
  
            `navigator.clipboard.writeText(getSubLink(username)).then(() =&gt; {`  
  
                `alert('✅ لینک ساب متنی با موفقیت کپی شد!');`  
  
            `}).catch(() =&gt; {`  
  
                `alert('خطا در کپی کردن لینک ساب!');`  
  
            `});`  
  
        `}`  
  
		`function toggleQrModal(show, text) {`  
  
            `const container = document.getElementById('qrcode-container');`  
  
            `if (show) {`  
  
                `container.innerHTML = '';`  
  
                `new QRCode(container, { text: text, width: 200, height: 200, colorDark: "#000000", colorLight: "#ffffff", correctLevel: QRCode.CorrectLevel.M });`  
  
            `}`  
  
            `setModalState('qr-modal', show);`  
  
        `}`  
  
        `function showSubQr(encodedUsername) {`  
  
            `const username = decodeURIComponent(encodedUsername);`  
  
            `const link = getSubLink(username);`  
  
            `toggleQrModal(true, link);`  
  
        `}`  
  
        `function copyStatusLink(encodedUsername) {`  
  
            `const username = decodeURIComponent(encodedUsername);`  
  
            `navigator.clipboard.writeText(getStatusLink(username)).then(() =&gt; {`  
  
                `alert('✅ لینک صفحه وضعیت با موفقیت کپی شد!');`  
  
            `}).catch(() =&gt; {`  
  
                `alert('خطا در کپی کردن لینک صفحه وضعیت!');`  
  
            `});`  
  
        `}`  
  
        `function copyConfig(encodedUsername) {`  
  
            `const username = decodeURIComponent(encodedUsername);`  
  
            `const link = getvIeesLink(username);`  
  
            `if (!link) return;`  
  
            `navigator.clipboard.writeText(link).then(() =&gt; {`  
  
                `alert('✅ کـانفـیگ vIees با موفقیت کپی شد!');`  
  
            `}).catch(() =&gt; {`  
  
                `alert('خطا در کپی کردن کـانفـیگ!');`  
  
            `});`  
  
        `}`  
  
`function editUser(encodedUsername) {`  
  
    `const username = decodeURIComponent(encodedUsername);`  
  
    `const user = window.allUsers.find(u =&gt; u.username === username);`  
  
    `if (!user) {`  
  
        `alert('کاربر یافت نشد!');`  
  
        `return;`  
  
    `}`  
  
    `isEditMode = true;`  
  
    `editingUsername = username;`  
  
    `document.getElementById('modal-title').innerText = 'ویرایش کاربر: ' + username;`  
  
    `document.getElementById('submit-btn').innerText = 'ذخیره تغییرات';`  
  
    `const nameInput = document.getElementById('input-name');`  
  
    `nameInput.value = username;`  
  
    `nameInput.disabled = false;`  
  
    `document.getElementById('input-limit').value = user.limit_gb || '';`  
  
    `document.getElementById('input-expiry').value = user.expiry_days || '';`  
  
    `document.getElementById('input-req-limit').value = user.limit_req || '';`  
  
    `document.getElementById('input-ip-limit').value = (user.ip_limit !== undefined &amp;&amp; user.ip_limit !== null) ? user.ip_limit : (user.max_connections || '');`  
  
    `document.getElementById('input-ips').value = user.ips || '';`  
  
    `document.getElementById('fingerprint-select').value = user.fingerprint || 'chrome';`  
  
	`document.getElementById('hidden-auto-rotate').value = [user.auto](http://user.auto)_rotate_ip || '0';`  
  
	`document.getElementById('hidden-rotate-time').value = user.rotate_time || '';`  
  
	`document.getElementById('hidden-ip-operator').value = user.ip_operator || 'all';`  
  
	`document.getElementById('hidden-ip-count').value = user.ip_count || '20';`  
  
    `document.getElementById('fingerprint-select').value = user.fingerprint || 'chrome';`  
  
    `document.getElementById('input-block-porn').checked = (user.block_porn === 1);`  
  
    `document.getElementById('input-block-ads').checked = (user.block_ads === 1);`  
  
    `const autoRotateUserProxyCheck = document.getElementById('input-auto-rotate-user-proxy');`  
  
    `if (autoRotateUserProxyCheck) autoRotateUserProxyCheck.checked = ([user.auto](http://user.auto)_rotate_user_proxy === 1);`  
  
    `const hasAutoReset = Boolean(([user.auto](http://user.auto)_reset_vol_days &amp;&amp; [user.auto](http://user.auto)_reset_vol_days &gt; 0) || ([user.auto](http://user.auto)_reset_req_days &amp;&amp; [user.auto](http://user.auto)_reset_req_days &gt; 0));`  
  
    `const autoResetToggle = document.getElementById('input-auto-reset-toggle');`  
  
    `if (autoResetToggle) autoResetToggle.checked = hasAutoReset;`  
  
    `document.getElementById('input-auto-reset-vol').value = hasAutoReset &amp;&amp; [user.auto](http://user.auto)_reset_vol_days &gt; 0 ? [user.auto](http://user.auto)_reset_vol_days : '';`  
  
    `document.getElementById('input-auto-reset-req').value = hasAutoReset &amp;&amp; [user.auto](http://user.auto)_reset_req_days &gt; 0 ? [user.auto](http://user.auto)_reset_req_days : '';`  
  
    `window.toggleAutoResetInputs(hasAutoReset);`  
  
    `const hasFrag = Boolean(user.frag_len &amp;&amp; user.frag_len !== "" &amp;&amp; user.frag_int &amp;&amp; user.frag_int !== "");`  
  
    `const fragToggle = document.getElementById('input-frag-toggle');`  
  
    `if (fragToggle) fragToggle.checked = hasFrag;`  
  
    `document.getElementById('input-frag-len').value = hasFrag ? user.frag_len : '200-3000';`  
  
    `document.getElementById('input-frag-int').value = hasFrag ? user.frag_int : '1-2';`  
  
    `window.toggleFragInputs(hasFrag);`  
  
    `const userPorts = String(user.port || '').split(',').map(p =&gt; p.trim());`  
  
    `const predefinedPorts = [...tlsPorts, ...nonTlsPorts];`  
  
    `const customPorts = userPorts.filter(p =&gt; !predefinedPorts.includes(p) &amp;&amp; p !== '');`  
  
    `document.querySelectorAll('input[name="ports"]').forEach(cb =&gt; {`  
  
        `cb.checked = userPorts.includes(cb.value);`  
  
    `});`  
  
    `const customPortInput = document.getElementById('input-custom-ports');`  
  
    `if (customPortInput) customPortInput.value = customPorts.join(' ');`  
  
    `const userProxyToggle = document.getElementById('user-proxy-mode-toggle');`  
  
    `const userLocSelect = document.getElementById('user-location-select');`  
  
    `const userLocSearch = document.getElementById('user-location-search');`  
  
    `const userSocksInput = document.getElementById('user-socks5-input');`  
  
    `if (userLocSearch) {`  
  
        `userLocSearch.value = '';`  
  
        `if (typeof window.filterUserLocations === 'function') window.filterUserLocations();`  
  
    `}`  
  
	`const targetProxy = user.user_socks5 || user.user_proxy_ip;`  
  
	`const userProxyResult = document.getElementById('test-user-proxy-result');`  
  
	`if (userProxyResult) userProxyResult.innerText = '';`  
  
	`if (targetProxy) {`  
  
		`if (userProxyToggle) userProxyToggle.checked = true;`  
  
		`if (typeof window.toggleUserProxyMode === 'function') window.toggleUserProxyMode(true);`  
  
		`if (userSocksInput) userSocksInput.value = targetProxy;`  
  
		`if (userLocSelect) userLocSelect.value = '';`  
  
	`} else {`  
  
		`if (userProxyToggle) userProxyToggle.checked = false;`  
  
		`if (typeof window.toggleUserProxyMode === 'function') window.toggleUserProxyMode(false);`  
  
		`if (userSocksInput) userSocksInput.value = '';`  
  
		`if (userLocSelect) userLocSelect.value = user.user_proxy_iata || '';`  
  
	`}`  
  
	`toggleModal(true);`  
  
`}`  
  
        `async function deleteUser(encodedUsername) {`  
  
			`const username = decodeURIComponent(encodedUsername);`  
  
			`if (await customConfirm('آیا از حذف کاربر ' + username + ' مطمئن هستید؟')) {`  
  
                `try {`  
  
                    `const response = await fetch('/api/users/' + encodeURIComponent(username), { method: 'DELETE' });`  
  
                    `if (response.ok) {`  
  
                        `alert('✅ کاربر با موفقیت حذف شد.');`  
  
                        `window.selectedUsernames.delete(username);`  
  
                        `await loadUsers(true);`  
  
                    `} else {`  
  
                        `const errData = await response.json();`  
  
                        `alert('خطا: ' + (errData.error || 'عملیات ناموفق بود'));`  
  
                    `}`  
  
                `} catch (err) {`  
  
                    `alert('خطا در برقراری ارتباط با سرور');`  
  
                `}`  
  
            `}`  
  
        `}`  
  
        `function getFlagEmoji(countryCode) {`  
  
            `if (!countryCode) return '🌐';`  
  
            `const codePoints = countryCode.toUpperCase().split('').map(char =&gt; 127397 + char.charCodeAt(0));`  
  
            `try {`  
  
                `return String.fromCodePoint(...codePoints);`  
  
            `} catch (e) {`  
  
                `return '🌐';`  
  
            `}`  
  
        `}`  
  
        `function renderLocationsUI(locations, activeIata) {`  
  
            `const select = document.getElementById('location-select');`  
  
            `const userSelect = document.getElementById('user-location-select');`  
  
            `locations.sort((a, b) =&gt; (a.cca2 || '').localeCompare(b.cca2 || ''));`  
  
            `let html = '&lt;option value=""&gt;🌐 پیش‌فرض (لوکیشن خودکار)&lt;/option&gt;';`  
  
            `let userHtml = '&lt;option value=""&gt;🌐 استفاده از تنظیمات عمومی پـنـل&lt;/option&gt;';`  
  
            `locations.forEach(loc =&gt; {`  
  
                `if (loc.iata &amp;&amp; [loc.city](http://loc.city)) {`  
  
                    `const flag = getFlagEmoji(loc.cca2);`  
  
                    `const isSelected = loc.iata.toUpperCase() === activeIata.toUpperCase() ? 'selected' : '';`  
  
                    `const optionStr = '&lt;option value="' + loc.iata + '" ' + isSelected + '&gt;' + flag + ' ' + [loc.city](http://loc.city) + ' (' + loc.iata + ')&lt;/option&gt;';`  
  
                    `html += optionStr;`  
  
                    `userHtml += '&lt;option value="' + loc.iata + '"&gt;' + flag + ' ' + [loc.city](http://loc.city) + ' (' + loc.iata + ')&lt;/option&gt;';`  
  
                `}`  
  
            `});`  
  
            `if (select) select.innerHTML = html;`  
  
            `if (userSelect) userSelect.innerHTML = userHtml;`  
  
        `}`  
  
`async function loadLocations() {`  
  
    `return;`  
  
`}`  
  
`function saveSettings() {`  
  
    `toggleSettingsModal(false);`  
  
    `showToast('✅ تنظیمات با موفقیت ذخیره شد.');`  
  
`}`  
  
`window.toggleUserProxyMode = function(isSocksMode) {`  
  
    `const socksContainer = document.getElementById('user-socks5-container');`  
  
    `const socksInput = document.getElementById('user-socks5-input');`  
  
    `if (isSocksMode) {`  
  
        `if (socksContainer) socksContainer.classList.remove('opacity-50', 'pointer-events-none');`  
  
        `if (socksInput) socksInput.disabled = false;`  
  
    `} else {`  
  
        `if (socksContainer) socksContainer.classList.add('opacity-50', 'pointer-events-none');`  
  
        `if (socksInput) socksInput.disabled = true;`  
  
    `}`  
  
`};`  
  
`async function loadProxyFlags() {`  
  
    `const badges = document.querySelectorAll('.async-proxy-flag');`  
  
    `if (badges.length === 0) return;`  
  
    `let cache = {};`  
  
    `try { cache = JSON.parse(localStorage.getItem('proxy_flag_cache') || '{}'); } catch(e) {}`  
  
    `for (let badge of badges) {`  
  
        `const proxyStr = badge.getAttribute('data-proxy');`  
  
        `if (!proxyStr) continue;`  
  
        `if (cache[proxyStr]) {`  
  
            `badge.innerHTML = cache[proxyStr];`  
  
            `badge.classList.remove('async-proxy-flag');`  
  
            `continue;`  
  
        `}`  
  
        `badge.classList.remove('async-proxy-flag');`  
  
        `try {`  
  
            `const controller = new AbortController();`  
  
            `const timeoutId = setTimeout(() =&gt; controller.abort(), 4000);`  
  
            `const res = await fetch('/api/test-proxy', {`  
  
                `method: 'POST',`  
  
                `headers: { 'Content-Type': 'application/json' },`  
  
                `body: JSON.stringify({ proxy: proxyStr }),`  
  
                `signal: controller.signal`  
  
            `});`  
  
            `clearTimeout(timeoutId);`  
  
            `const data = await res.json();`  
  
            `let flag = '🌐';`  
  
            `if (res.ok &amp;&amp; data.success &amp;&amp; [data.country](http://data.country)) {`  
  
                `flag = typeof getFlagEmoji === 'function' ? getFlagEmoji([data.country](http://data.country)) : '🌐';`  
  
            `}`  
  
            `cache[proxyStr] = flag;`  
  
            `localStorage.setItem('proxy_flag_cache', JSON.stringify(cache));`  
  
            `badge.innerHTML = flag;`  
  
        `} catch (e) {`  
  
            `badge.innerHTML = '🌐';`  
  
        `}`  
  
    `}`  
  
`}`  
  
`window.filterUserLocations = function() {`  
  
    `const searchTerm = document.getElementById('user-location-search').value.toLowerCase().trim();`  
  
    `const cachedLocations = localStorage.getItem('cached_locations_list');`  
  
    `if (!cachedLocations) return;`  
  
    `try {`  
  
        `const allLocations = JSON.parse(cachedLocations);`  
  
        `const filteredLocations = allLocations.filter(loc =&gt; {`  
  
            `if (!loc.iata || ![loc.city](http://loc.city)) return false;`  
  
            `const searchString = (loc.iata + ' ' + [loc.city](http://loc.city) + ' ' + (loc.cca2 || '')).toLowerCase();`  
  
            `return searchString.includes(searchTerm);`  
  
        `});`  
  
        `const userSelect = document.getElementById('user-location-select');`  
  
        `let userHtml = '&lt;option value=""&gt;🌐 استفاده از تنظیمات عمومی پـنـل&lt;/option&gt;';`  
  
        `filteredLocations.forEach(loc =&gt; {`  
  
            `const flag = getFlagEmoji(loc.cca2);`  
  
            `userHtml += '&lt;option value="' + loc.iata + '"&gt;' + flag + ' ' + [loc.city](http://loc.city) + ' (' + loc.iata + ')&lt;/option&gt;';`  
  
        `});`  
  
        `if (userSelect) userSelect.innerHTML = userHtml;`  
  
    `} catch(e) {}`  
  
`};`  
  
`async function testUserSocksProxy() {`  
  
	`const btn = document.getElementById('test-user-proxy-btn');`  
  
	`const resultSpan = document.getElementById('test-user-proxy-result');`  
  
	`const proxyStr = document.getElementById('user-socks5-input').value.trim();`  
  
	`if (!proxyStr) {`  
  
		`resultSpan.innerText = 'وارد نشده!';`  
  
		`resultSpan.className = 'text-[11px] font-bold text-red-500 w-full mt-1';`  
  
		`return;`  
  
	`}`  
  
	`btn.disabled = true;`  
  
	`btn.innerText = 'صبر کنید...';`  
  
	`resultSpan.innerText = '';`  
  
	`const controller = new AbortController();`  
  
	`const timeoutId = setTimeout(() =&gt; controller.abort(), 5000);`  
  
	`try {`  
  
		`const res = await fetch('/api/test-proxy', {`  
  
			`method: 'POST',`  
  
			`headers: { 'Content-Type': 'application/json' },`  
  
			`body: JSON.stringify({ proxy: proxyStr }),`  
  
			`signal: controller.signal`  
  
		`});`  
  
		`clearTimeout(timeoutId);`  
  
		`const data = await res.json();`  
  
		`if (res.ok &amp;&amp; data.success) {`  
  
			`const flag = typeof getFlagEmoji === 'function' ? getFlagEmoji([data.country](http://data.country)) : '🌐';`  
  
			`resultSpan.innerText = flag + ' پینگ: ' + [data.ping](http://data.ping) + 'ms';`  
  
			`resultSpan.className = 'text-[11px] font-bold text-green-600';`  
  
		`} else {`  
  
			`resultSpan.innerText = 'خطا: ' + (data.error || 'ناموفق');`  
  
			`resultSpan.className = 'text-[11px] font-bold text-red-500 w-full mt-1 break-words';`  
  
		`}`  
  
	`} catch (e) {`  
  
		`clearTimeout(timeoutId);`  
  
		`if ([e.name](http://e.name) === 'AbortError') resultSpan.innerText = 'تایم‌اوت (پـروکـسـی خراب است)';`  
  
		`else resultSpan.innerText = 'خطا در ارتباط';`  
  
		`resultSpan.className = 'text-[11px] font-bold text-red-500 w-full mt-1 break-words';`  
  
	`} finally {`  
  
		`btn.disabled = false;`  
  
		`btn.innerText = 'تست پـروکـسـی';`  
  
	`}`  
  
`}`  
  
        `async function exportUsersBackup() {`  
  
            `if (!window.allUsers || window.allUsers.length === 0) {`  
  
                `alert('⚠️ کاربری برای پشتیبان‌گیری وجود ندارد!');`  
  
                `return;`  
  
            `}`  
  
            `try {`  
  
                `const settingsRes = await fetch('/api/settings/bulk');`  
  
                `const settingsData = await settingsRes.json();`  
  
                `const backupData = {`  
  
                    `users: window.allUsers,`  
  
                    `settings: settingsData`  
  
                `};`  
  
                `const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(backupData, null, 2));`  
  
                `const downloadAnchor = document.createElement('a');`  
  
                `const dateStr = new Date().toISOString().split('T')[0];`  
  
                `downloadAnchor.setAttribute("href", dataStr);`  
  
                `downloadAnchor.setAttribute("download", "zeus_full_backup_" + dateStr + ".json");`  
  
                `document.body.appendChild(downloadAnchor);`  
  
                `[downloadAnchor.click](http://downloadAnchor.click)();`  
  
                `downloadAnchor.remove();`  
  
            `} catch (err) {`  
  
                `alert('❌ خطا در دریافت تنظیمات برای بک‌آپ.');`  
  
            `}`  
  
        `}`  
  
        `function triggerImportBackup() {`  
  
            `document.getElementById('backup-file-input').click();`  
  
        `}`  
  
        `async function importUsersBackup(event) {`  
  
            `const file = [event.target](http://event.target).files[0];`  
  
            `if (!file) return;`  
  
            `const reader = new FileReader();`  
  
            `reader.onload = async function(e) {`  
  
                `const importBtn = document.querySelector('button[onclick="triggerImportBackup()"]');`  
  
                `const exportBtn = document.querySelector('button[onclick="exportUsersBackup()"]');`  
  
                `const closeBtn = document.querySelector('#settings-modal button[onclick="toggleSettingsModal(false)"]');`  
  
                `try {`  
  
                    `const parsedData = JSON.parse([e.target](http://e.target).result);`  
  
                    `let backupUsers = [];`  
  
                    `let backupSettings = null;`  
  
                    `if (Array.isArray(parsedData)) {`  
  
                        `backupUsers = parsedData;`  
  
                    `} else if (parsedData &amp;&amp; parsedData.users &amp;&amp; Array.isArray(parsedData.users)) {`  
  
                        `backupUsers = parsedData.users;`  
  
                        `backupSettings = parsedData.settings;`  
  
                    `} else {`  
  
                        `alert('❌ فایل پشتیبان نامعتبر است!');`  
  
                        `return;`  
  
                    `}`  
  
                    `const validBackupUsers = backupUsers.filter(u =&gt; u &amp;&amp; typeof u === 'object' &amp;&amp; u.username);`  
  
                    `if (validBackupUsers.length === 0 &amp;&amp; !backupSettings) {`  
  
                        `alert('❌ هیچ داده معتبری در فایل یافت نشد!');`  
  
                        `return;`  
  
                    `}`  
  
                    `if (backupSettings &amp;&amp; Object.keys(backupSettings).length &gt; 0) {`  
  
                        `const restoreSettings = await customConfirm('⚙️ فایل بک‌آپ شامل تنظیمات پـنـل نیز می‌باشد. آیا می‌خواهید تنظیمات هم بازگردانی شوند؟');`  
  
                        `if (restoreSettings) {`  
  
                            `try {`  
  
                                `await fetch('/api/settings/bulk', {`  
  
                                    `method: 'POST',`  
  
                                    `headers: { 'Content-Type': 'application/json' },`  
  
                                    `body: JSON.stringify({ settings: backupSettings })`  
  
                                `});`  
  
                            `} catch (err) {}`  
  
                        `}`  
  
                    `}`  
  
                    `const existingUsernames = new Set((window.allUsers || []).map(u =&gt; u.username));`  
  
                    `const duplicates = validBackupUsers.filter(u =&gt; existingUsernames.has(u.username));`  
  
                    `let overwrite = false;`  
  
                    `if (duplicates.length &gt; 0) {`  
  
                        `overwrite = await customConfirm('⚠️ تعداد ' + duplicates.length + ' کاربر تکراری شناسایی شد. آیا می‌خواهید اطلاعات آن‌ها بازنویسی شود؟');`  
  
                    `}`  
  
                    `if (importBtn) importBtn.disabled = true;`  
  
                    `if (exportBtn) exportBtn.disabled = true;`  
  
                    `if (closeBtn) closeBtn.disabled = true;`  
  
                    `let successCount = 0;`  
  
                    `let currentStep = 0;`  
  
                    `for (const u of validBackupUsers) {`  
  
                        `currentStep++;`  
  
                        `if (importBtn) {`  
  
                            `importBtn.innerText = '⏳ بازیابی (' + currentStep + '/' + validBackupUsers.length + ')';`  
  
                        `}`  
  
                        `const exists = existingUsernames.has(u.username);`  
  
                        `if (exists) {`  
  
                            `if (overwrite) {`  
  
                                `try {`  
  
                                    `await fetch('/api/users/' + encodeURIComponent(u.username), { method: 'DELETE' });`  
  
                                    `const res = await fetch('/api/users', {`  
  
                                        `method: 'POST',`  
  
                                        `headers: { 'Content-Type': 'application/json' },`  
  
                                        `body: JSON.stringify({`  
  
                                            `username: u.username,`  
  
                                            `uuid: u.uuid,`  
  
                                            `limit_gb: u.limit_gb,`  
  
                                            `expiry_days: u.expiry_days,`  
  
                                            `limit_req: u.limit_req,`  
  
                                            `ips: u.ips,`  
  
                                            `tls: u.tls,`  
  
                                            `port: u.port,`  
  
                                            `fingerprint: u.fingerprint,`  
  
                                            `ip_limit: u.ip_limit !== undefined ? u.ip_limit : u.max_connections,`  
  
                                            `used_gb: u.used_gb,`  
  
                                            `used_req: u.used_req,`  
  
                                            `created_at: u.created_at,`  
  
                                            `is_active: [u.is](http://u.is)_active,`  
  
                                            `block_porn: u.block_porn,`  
  
                                            `block_ads: u.block_ads,`  
  
                                            `frag_len: u.frag_len,`  
  
                                            `frag_int: u.frag_int`  
  
                                        `})`  
  
                                    `});`  
  
                                    `if (res.ok) successCount++;`  
  
                                `} catch(err) {}`  
  
                            `}`  
  
                        `} else {`  
  
                            `try {`  
  
                                `const res = await fetch('/api/users', {`  
  
                                    `method: 'POST',`  
  
                                    `headers: { 'Content-Type': 'application/json' },`  
  
                                    `body: JSON.stringify({`  
  
                                        `username: u.username,`  
  
                                        `uuid: u.uuid,`  
  
                                        `limit_gb: u.limit_gb,`  
  
                                        `expiry_days: u.expiry_days,`  
  
                                        `limit_req: u.limit_req,`  
  
                                        `ips: u.ips,`  
  
                                        `tls: u.tls,`  
  
                                        `port: u.port,`  
  
                                        `fingerprint: u.fingerprint,`  
  
                                        `ip_limit: u.ip_limit !== undefined ? u.ip_limit : u.max_connections,`  
  
                                        `used_gb: u.used_gb,`  
  
                                        `used_req: u.used_req,`  
  
                                        `created_at: u.created_at,`  
  
                                        `is_active: [u.is](http://u.is)_active,`  
  
                                        `block_porn: u.block_porn,`  
  
                                        `block_ads: u.block_ads,`  
  
                                        `frag_len: u.frag_len,`  
  
                                        `frag_int: u.frag_int`  
  
                                    `})`  
  
                                `});`  
  
                                `if (res.ok) successCount++;`  
  
                            `} catch(err) {}`  
  
                        `}`  
  
                    `}`  
  
                    `alert('✅ عملیات بازیابی با موفقیت انجام شد. صفحه رفرش می‌شود...');`  
  
                    `setTimeout(() =&gt; { window.location.reload(); }, 1500);`  
  
                `} catch(err) {`  
  
                    `alert('❌ خطا در خواندن یا پردازش فایل پشتیبان!');`  
  
                `} finally {`  
  
                    `if (importBtn) {`  
  
                        `importBtn.disabled = false;`  
  
                        `importBtn.innerText = '📥 بازیابی';`  
  
                    `}`  
  
                    `if (exportBtn) exportBtn.disabled = false;`  
  
                    `if (closeBtn) closeBtn.disabled = false;`  
  
                    `[event.target](http://event.target).value = '';`  
  
                `}`  
  
            `};`  
  
            `reader.readAsText(file);`  
  
        `}`  
  
        `async function changeAdminPassword() {`  
  
            `const currentPwd = document.getElementById('change-pwd-current').value;`  
  
            `const newPwd = document.getElementById('change-pwd-new').value;`  
  
            `const btn = document.getElementById('change-pwd-btn');`  
  
            `if (!currentPwd || !newPwd) {`  
  
                `alert('⚠️ وارد کردن رمز عبور فعلی و جدید الزامی است!');`  
  
                `return;`  
  
            `}`  
  
            `if (newPwd.length &lt; 4) {`  
  
                `alert('⚠️ رمز عبور جدید باید حداقل ۴ کاراکتر باشد!');`  
  
                `return;`  
  
            `}`  
  
            `btn.disabled = true;`  
  
            `btn.innerText = 'در حال تغییر...';`  
  
            `try {`  
  
                `const response = await fetch('/api/change-password', {`  
  
                    `method: 'POST',`  
  
                    `headers: { 'Content-Type': 'application/json' },`  
  
                    `body: JSON.stringify({ current_password: currentPwd, new_password: newPwd })`  
  
                `});`  
  
                `const data = await response.json();`  
  
                `if (response.ok &amp;&amp; data.success) {`  
  
                    `alert('✅ رمز عبور با موفقیت تغییر کرد.');`  
  
                    `document.getElementById('change-pwd-current').value = '';`  
  
                    `document.getElementById('change-pwd-new').value = '';`  
  
                    `toggleSettingsModal(false);`  
  
                `} else {`  
  
                    `alert('❌ خطا: ' + (data.error || 'عملیات ناموفق بود'));`  
  
                `}`  
  
            `} catch (err) {`  
  
                `alert('خطا در برقراری ارتباط با سرور');`  
  
            `} finally {`  
  
                `btn.disabled = false;`  
  
                `btn.innerText = 'تغییر رمز عبور';`  
  
            `}`  
  
        `}`  
  
        `async function logoutAdmin() {`  
  
			`if (await customConfirm('آیا می‌خواهید از پـنـل خارج شوید؟ ⚠️ ')) {`  
  
                `try {`  
  
                    `await fetch('/api/logout', { method: 'POST' });`  
  
                `} catch (err) {}`  
  
                `window.location.reload();`  
  
            `}`  
  
        `}`  
  
`const CURRENT_VERSION = '1.9.12';`  
  
`const UPDATE_FIX = "constsCURRENT_VERSION='d.d.d'";`  
  
		`async function checkForUpdates(isManual = false) {`  
  
            `try {`  
  
                `if (isManual) {`  
  
                    `document.getElementById('update-toggle').classList.add('animate-pulse');`  
  
                `}`  
  
                `const res = await fetch('[https://zeus-files.surge.sh/panel-source?t=](https://zeus-files.surge.sh/panel-source?t=)' + [Date.now](http://Date.now)());`  
  
                `if (!res.ok) throw new Error('Network response was not ok');`  
  
                `const text = await res.text();`  
  
                `const match = text.match(/const\\s+CURRENT_VERSION\\s*=\\s*['"](\\d+\\.\\d+\\.\\d+)['"]/i);`  
  
                `const latestVersion = match ? match[1] : null;`  
  
                `if (isManual) {`  
  
                    `document.getElementById('update-toggle').classList.remove('animate-pulse');`  
  
                `}`  
  
                `if (latestVersion &amp;&amp; latestVersion !== CURRENT_VERSION) {`  
  
                    `document.getElementById('update-toggle').className = "p-2 rounded-md bg-red-100 dark:bg-red-900/60 border border-red-500 hover:bg-red-200 dark:hover:bg-red-900/80 transition text-red-700 dark:text-red-400 shadow-[0_0_15px_rgba(239,68,68,0.6)] animate-pulse relative";`  
  
                    `const badge = document.getElementById('update-badge');`  
  
                    `if (badge) badge.remove();`  
  
                    `if (isManual) {`  
  
                        `toggleUpdateModal(true, latestVersion);`  
  
                    `}`  
  
                `} else {`  
  
                    `if (isManual) {`  
  
                        `alert('شما در حال استفاده از آخرین نسخه (v' + CURRENT_VERSION + ') هستید.');`  
  
                    `}`  
  
                `}`  
  
            `} catch (err) {`  
  
                `if (isManual) {`  
  
                    `document.getElementById('update-toggle').classList.remove('animate-pulse');`  
  
                    `alert('خطا در بررسی آپدیت از گیت هاب.');`  
  
                `}`  
  
            `}`  
  
        `}`  
  
        `function toggleTokenModal(show) {`  
  
            `setModalState('token-modal', show);`  
  
            `if (!show) document.getElementById('update-token-input').value = '';`  
  
        `}`  
  
        `function submitTokenForUpdate() {`  
  
            `const token = document.getElementById('update-token-input').value.trim();`  
  
            `if (!token) {`  
  
                `alert('لطفاً توکن را وارد کنید.');`  
  
                `return;`  
  
            `}`  
  
            `toggleTokenModal(false);`  
  
            `handleCoreAction(window.pendingCoreAction || 'update', token);`  
  
        `}`  
  
        `async function applyUpdate(token = null) {`  
  
            `await handleCoreAction('update', token);`  
  
        `}`  
  
`let cachedIpsData = {};`  
  
`async function fetchIpsList() {`  
  
    `try {`  
  
        `const response = await fetchWithFallbackUI('ips.txt');`  
  
        `if (!response.ok) throw new Error('Fetch failed');`  
  
        `const text = await response.text();`  
  
        `const blocks = text.split('----------');`  
  
        `cachedIpsData = {};`  
  
        `blocks.forEach(block =&gt; {`  
  
            `const lines = block.trim().split('\\n').map(l =&gt; l.trim()).filter(l =&gt; l.length &gt; 0);`  
  
            `if (lines.length === 0) return;`  
  
            `let opName = "Unknown";`  
  
            `const ips = [];`  
  
            `lines.forEach(line =&gt; {`  
  
                `if (line.includes('#')) {`  
  
                    `opName = line.split('#')[1].trim();`  
  
                `} else if (!line.startsWith('[source')) {`  
  
                    `ips.push(line);`  
  
                `}`  
  
            `});`  
  
            `if (ips.length &gt; 0) {`  
  
                `cachedIpsData[opName] = ips;`  
  
            `}`  
  
        `});`  
  
        `populateIpSelect();`  
  
    `} catch (err) {`  
  
        `alert('Failed to load IP list from GitHub.');`  
  
        `toggleIpSelectorModal(false);`  
  
    `}`  
  
`}`  
  
`function populateIpSelect() {`  
  
    `const select = document.getElementById('ip-operator-select');`  
  
    `select.innerHTML = '&lt;option value="all"&gt;همه (توصیه شده)&lt;/option&gt;';`  
  
    `Object.keys(cachedIpsData).forEach(op =&gt; {`  
  
        `const option = document.createElement('option');`  
  
        `option.value = op;`  
  
        `option.textContent = op;`  
  
        `select.appendChild(option);`  
  
    `});`  
  
`}`  
  
`function toggleIpSelectorModal(show) {`  
  
    `setModalState('ip-selector-modal', show);`  
  
    `if (!show) {`  
  
		`const rotateToggle = document.getElementById('input-auto-rotate-ip-toggle');`  
  
		`if (rotateToggle) rotateToggle.checked = false;`  
  
		`const rotateTime = document.getElementById('input-auto-rotate-ip-time');`  
  
		`if (rotateTime) rotateTime.value = '';`  
  
		`if (typeof window.toggleAutoRotateIpInputs === 'function') window.toggleAutoRotateIpInputs(false);`  
  
    `}`  
  
`}`  
  
`async function openIpSelectorModal() {`  
  
    `toggleIpSelectorModal(true);`  
  
    `document.getElementById('ip-loading-state').classList.remove('hidden');`  
  
    `document.getElementById('ip-selection-form').classList.add('hidden');`  
  
    `await fetchIpsList();`  
  
	`const op = document.getElementById('hidden-ip-operator').value;`  
  
	`const selectOp = document.getElementById('ip-operator-select');`  
  
	`if (selectOp.querySelector('option[value="' + op + '"]')) {`  
  
		`selectOp.value = op;`  
  
	`} else {`  
  
		`selectOp.value = 'all';`  
  
	`}`  
  
	`document.getElementById('ip-count-input').value = document.getElementById('hidden-ip-count').value || 20;`  
  
	`const isAuto = document.getElementById('hidden-auto-rotate').value === '1';`  
  
	`document.getElementById('input-auto-rotate-ip-toggle').checked = isAuto;`  
  
	`document.getElementById('input-auto-rotate-ip-time').value = document.getElementById('hidden-rotate-time').value;`  
  
	`if (typeof window.toggleAutoRotateIpInputs === 'function') window.toggleAutoRotateIpInputs(isAuto);`  
  
    `document.getElementById('ip-loading-state').classList.add('hidden');`  
  
    `document.getElementById('ip-selection-form').classList.remove('hidden');`  
  
`}`  
  
`function applySelectedIps() {`  
  
    `const operator = document.getElementById('ip-operator-select').value;`  
  
    `let count = parseInt(document.getElementById('ip-count-input').value, 10);`  
  
    `if (isNaN(count) || count &lt; 1) count = 10;`  
  
    `let availableIps = [];`  
  
    `if (operator === 'all') {`  
  
        `Object.values(cachedIpsData).forEach(ips =&gt; {`  
  
            `availableIps = availableIps.concat(ips);`  
  
        `});`  
  
    `} else {`  
  
        `availableIps = cachedIpsData[operator] || [];`  
  
    `}`  
  
    `availableIps = [...new Set(availableIps)];`  
  
    `let selectedIps = [];`  
  
    `if (count &gt;= availableIps.length) {`  
  
        `selectedIps = availableIps;`  
  
    `} else {`  
  
        `const shuffled = availableIps.slice();`  
  
        `for (let i = shuffled.length - 1; i &gt; 0; i--) {`  
  
            `const j = Math.floor(Math.random()  (i + 1));`  
  
            `[shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];`  
  
        `}`  
  
        `selectedIps = shuffled.slice(0, count);`  
  
    `}`  
  
    `document.getElementById('input-ips').value = selectedIps.join('\\n');`  
  
	`document.getElementById('hidden-auto-rotate').value = document.getElementById('input-auto-rotate-ip-toggle').checked ? '1' : '0';`  
  
	`document.getElementById('hidden-rotate-time').value = document.getElementById('input-auto-rotate-ip-time').value || '';`  
  
	`document.getElementById('hidden-ip-operator').value = operator;`  
  
	`document.getElementById('hidden-ip-count').value = count;`  
  
    `toggleIpSelectorModal(false);`  
  
`}`  
  
`document.addEventListener('DOMContentLoaded', () =&gt; {`  
  
			`setTimeout(() =&gt; {`  
  
   			 `const freeModal = document.getElementById('free-panel-warning-modal');`  
  
    			`const freeCard = freeModal.querySelector('div');`  
  
    			`freeModal.classList.remove('opacity-0', 'pointer-events-none');`  
  
    			`freeModal.classList.add('opacity-100', 'pointer-events-auto');`  
  
    			`freeCard.classList.remove('opacity-0', 'scale-95');`  
  
    			`freeCard.classList.add('opacity-100', 'scale-100');`  
  
			`}, 3000);`  
  
            `const versionBadge = document.getElementById('panel-version');`  
  
            `if (versionBadge) versionBadge.innerText = 'v' + CURRENT_VERSION;`  
  
            `renderPortCheckboxes();`  
  
            `loadUsers();`  
  
            `loadLocations();`  
  
            `window.usersRefreshIntervalId = null;`  
  
            `window.startRefreshInterval = function(intervalMs) {`  
  
                `if (window.usersRefreshIntervalId) {`  
  
                    `clearInterval(window.usersRefreshIntervalId);`  
  
                `}`  
  
                `window.usersRefreshIntervalId = setInterval(() =&gt; {`  
  
                    `if (!document.hidden) loadUsers(true);`  
  
                `}, intervalMs);`  
  
            `};`  
  
            `window.changeRefreshRate = function(val) {`  
  
                `const ms = parseInt(val, 10);`  
  
                `localStorage.setItem('zeus_refresh_rate', ms);`  
  
                `window.startRefreshInterval(ms);`  
  
                `showToast('نرخ رفرش پـنـل تغییر کرد');`  
  
            `};`  
  
            `const savedRate = localStorage.getItem('zeus_refresh_rate');`  
  
            `const initialRate = savedRate ? parseInt(savedRate, 10) : 10000;`  
  
            `const selectEl = document.getElementById('refresh-rate-select');`  
  
            `if (selectEl) {`  
  
                `selectEl.value = String(initialRate);`  
  
            `}`  
  
            `window.startRefreshInterval(initialRate);`  
  
			`setTimeout(() =&gt; checkForUpdates(false), 1000);`  
  
            `setInterval(() =&gt; {`  
  
                `if (!document.hidden) checkForUpdates(false);`  
  
            `}, 300000);`  
  
            `setTimeout(() =&gt; checkGlobalMessage(), 50);`  
  
            `setInterval(() =&gt; {`  
  
                `if (!document.hidden) checkGlobalMessage();`  
  
            `}, 300000);`  
  
            `window.addEventListener('mousedown', (e) =&gt; {`  
  
                `window._modalMouseDownTarget = [e.target](http://e.target);`  
  
            `});`  
  
            `window.addEventListener('click', (e) =&gt; {`  
  
                `if (window._modalMouseDownTarget &amp;&amp; window._modalMouseDownTarget !== [e.target](http://e.target)) return;`  
  
                `if ([e.target.id](http://e.target.id) === 'user-modal') toggleModal(false);`  
  
                `if ([e.target.id](http://e.target.id) === 'ip-selector-modal') toggleIpSelectorModal(false);`  
  
                `if ([e.target.id](http://e.target.id) === 'settings-modal') toggleSettingsModal(false);`  
  
                `if ([e.target.id](http://e.target.id) === 'update-modal') toggleUpdateModal(false);`  
  
                `if ([e.target.id](http://e.target.id) === 'token-modal') toggleTokenModal(false);`  
  
                `if ([e.target.id](http://e.target.id) === 'qr-modal') toggleQrModal(false);`  
  
                `if ([e.target.id](http://e.target.id) === 'usage-warning-modal') closeUsageWarning();`  
  
                `if ([e.target.id](http://e.target.id) === 'free-panel-warning-modal') closeFreePanelWarning();`  
  
                `if ([e.target.id](http://e.target.id) === 'global-message-modal') {`  
  
                    `const closeBtn = document.getElementById('global-message-close-btn');`  
  
                    `if (closeBtn) [closeBtn.click](http://closeBtn.click)();`  
  
                `}`  
  
                `if ([e.target.id](http://e.target.id) === 'custom-confirm-modal') {`  
  
                    `const cancelBtn = document.getElementById('custom-confirm-cancel');`  
  
                    `if (cancelBtn) [cancelBtn.click](http://cancelBtn.click)();`  
  
                `}`  
  
            `});`  
  
        `});`  
  
`function toggleProxySelectorModal(show) { setModalState('proxy-selector-modal', show); }`  
  
		`async function loadVipCountries() {`  
  
			`const select = document.getElementById('vip-country-select');`  
  
			`const btn = document.getElementById('vip-fetch-btn');`  
  
			`select.innerHTML = '&lt;option value=""&gt;در حال بررسی مخزن...&lt;/option&gt;';`  
  
			`try {`  
  
				`const res = await fetchWithFallbackUI('vip-list');`  
  
				`if (!res.ok) throw new Error('API Error');`  
  
				`const data = await res.json();`  
  
				`const validCountries = data`  
  
					`.filter(function(file) { return [file.name](http://file.name).endsWith('.txt'); })`  
  
					`.map(function(file) { return [file.name](http://file.name).replace('.txt', '').toUpperCase(); });`  
  
				`if (validCountries.length === 0) throw new Error('Empty');`  
  
				`select.innerHTML = '&lt;option value=""&gt;یک کشور VIP انتخاب کنید...&lt;/option&gt;';`  
  
				`validCountries.forEach(function(country) {`  
  
					`const option = document.createElement('option');`  
  
					`option.value = country;`  
  
					`const flag = typeof getFlagEmoji === 'function' ? getFlagEmoji(country) : '🌐';`  
  
					`option.textContent = flag + ' ' + country;`  
  
					`select.appendChild(option);`  
  
				`});`  
  
				`btn.disabled = false;`  
  
			`} catch (err) {`  
  
				`select.innerHTML = '&lt;option value=""&gt;پـروکـسـی اختصاصی موجود نیست&lt;/option&gt;';`  
  
				`btn.disabled = true;`  
  
			`}`  
  
		`}`  
  
		`async function loadVipProxy() {`  
  
			`const select = document.getElementById('vip-country-select');`  
  
			`const country = select.value;`  
  
			`const btn = document.getElementById('vip-fetch-btn');`  
  
			`if (!country) return;`  
  
			`btn.disabled = true;`  
  
			`btn.innerText = '...';`  
  
			`try {`  
  
				`const res = await fetchWithFallbackUI('proxy_vip/' + country + '.txt?t=' + [Date.now](http://Date.now)());`  
  
				`if (!res.ok) throw new Error('فایل یافت نشد');`  
  
				`const text = await res.text();`  
  
				`const lines = text.split('\\n').map(function(l) { return l.trim(); }).filter(function(l) { return l.length &gt; 5; });`  
  
				`if (lines.length &gt; 0) {`  
  
					`const randomProxy = lines[Math.floor(Math.random()  lines.length)];`  
  
					`document.getElementById('user-socks5-input').value = randomProxy;`  
  
					`const userProxyResult = document.getElementById('test-user-proxy-result');`  
  
					`if (userProxyResult) {`  
  
					    `userProxyResult.innerText = '';`  
  
					`}`  
  
					`toggleProxySelectorModal(false);`  
  
					`showToast('✅ پـروکـسـی اختصاصی با موفقیت اعمال شد.');`  
  
                    `testUserSocksProxy();`  
  
				`} else {`  
  
					`alert('فایل پـروکـسـی این کشور خالی است.');`  
  
				`}`  
  
			`} catch (e) {`  
  
				`alert('خطا در دریافت پـروکـسـی اختصاصی.');`  
  
			`} finally {`  
  
				`btn.disabled = false;`  
  
				`btn.innerText = 'دریافت';`  
  
			`}`  
  
		`}`  
  
		`async function openProxySelectorModal() {`  
  
			`toggleProxySelectorModal(true);`  
  
			`const select = document.getElementById('proxy-country-select');`  
  
			`const fetchBtn = document.getElementById('proxy-fetch-btn');`  
  
			`const countriesList = [`  
  
		  `"AE", "AF", "AG", "AI", "AL", "AM", "AO", "AQ", "AR",`  
  
		  `"AS", "AT", "AU", "AW", "AX", "AZ", "BA", "BB", "BD", "BE",`  
  
		  `"BF", "BG", "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BQ",`  
  
		  `"BR", "BS", "BT", "BV", "BW", "BY", "BZ", "CA", "CC", "CD",`  
  
		  `"CF", "CG", "CH", "CI", "CK", "CL", "CM", "CN", "CO", "CR",`  
  
		  `"CU", "CV", "CW", "CX", "CY", "CZ", "DE", "DJ", "DK", "DM",`  
  
		  `"DO", "DZ", "EC", "EE", "EG", "EH", "ER", "ES", "ET", "FI",`  
  
		  `"FJ", "FK", "FM", "FO", "FR", "GA", "GB", "GD", "GE", "GF",`  
  
		  `"GG", "GH", "GI", "GL", "GM", "GN", "GP", "GQ", "GR", "GS",`  
  
		  `"GT", "GU", "GW", "GY", "HK", "HM", "HN", "HR", "HT", "HU",`  
  
		  `"ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IR", "IS", "IT",`  
  
		  `"JE", "JM", "JO", "JP", "KE", "KG", "KH", "KI", "KM", "KN",`  
  
		  `"KP", "KR", "KW", "KY", "KZ", "LA", "LB", "LC", "LI", "LK",`  
  
		  `"LR", "LS", "LT", "LU", "LV", "LY", "MA", "MC", "MD", "ME",`  
  
		  `"MF", "MG", "MH", "MK", "ML", "MM", "MN", "MO", "MP", "MQ",`  
  
		  `"MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ", "NA",`  
  
		  `"NC", "NE", "NF", "NG", "NI", "NL", "NO", "NP", "NR", "NU",`  
  
		  `"NZ", "OM", "PA", "PE", "PF", "PG", "PH", "PK", "PL", "PM",`  
  
		  `"PN", "PR", "PS", "PT", "PW", "PY", "QA", "RE", "RO", "RS",`  
  
		  `"RU", "RW", "SA", "SB", "SC", "SD", "SE", "SG", "SH", "SI",`  
  
		  `"SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS", "ST", "SV",`  
  
		  `"SX", "SY", "SZ", "TC", "TD", "TF", "TG", "TH", "TJ", "TK",`  
  
		  `"TL", "TM", "TN", "TO", "TR", "TT", "TV", "TW", "TZ", "UA",`  
  
		  `"UG", "UM", "US", "UY", "UZ", "VA", "VC", "VE", "VG", "VI",`  
  
		  `"VN", "VU", "WF", "WS", "YE", "YT", "ZA", "ZM", "ZW"`  
  
			`];`  
  
			`select.innerHTML = '';`  
  
			`countriesList.forEach(function(country) {`  
  
				`const option = document.createElement('option');`  
  
				`option.value = country;`  
  
				`const flag = typeof getFlagEmoji === 'function' ? getFlagEmoji(country) : '🌐';`  
  
				`option.textContent = flag + ' ' + country;`  
  
				`select.appendChild(option);`  
  
			`});`  
  
			`fetchBtn.disabled = false;`  
  
			`loadVipCountries();`  
  
		`}`  
  
`async function fetchAndLoadProxy() {`  
  
    `const select = document.getElementById("proxy-country-select");`  
  
    `const country = select.value;`  
  
    `if (!country) return;`  
  
    `const loadingState = document.getElementById("proxy-loading-state");`  
  
    `const formState = document.getElementById("proxy-selection-form");`  
  
    `const fetchBtn = document.getElementById("proxy-fetch-btn");`  
  
    `loadingState.classList.remove("hidden");`  
  
    `loadingState.innerText = "در حال دریافت لیست پـروکـسـی‌ها...";`  
  
    `formState.classList.add("hidden");`  
  
    `fetchBtn.disabled = true;`  
  
    `try {`  
  
        `const sources = [`  
  
            `{ url: "proxy/" + country.toUpperCase() + ".txt", prefix: "" }`  
  
        `];`  
  
        `const responses = await Promise.allSettled([sources.map](http://sources.map)(src =&gt; `  
  
            `fetchWithFallbackUI(src.url).then(async res =&gt; {`  
  
                `if (!res.ok) throw new Error();`  
  
                `const text = await res.text();`  
  
                `return { text: text, prefix: src.prefix };`  
  
            `})`  
  
        `));`  
  
        `let combinedProxies = [];`  
  
        `for (const res of responses) {`  
  
            `if (res.status === "fulfilled" &amp;&amp; res.value &amp;&amp; res.value.text) {`  
  
                `const rawLines = res.value.text.split("\\n");`  
  
                `for (let line of rawLines) {`  
  
                    `line = line.trim();`  
  
                    `if (line.length &gt; 5) {`  
  
                        `combinedProxies.push(line);`  
  
                    `}`  
  
                `}`  
  
            `}`  
  
        `}`  
  
        ``  
  
        `// 🛠 اصلاح هوشمند: اگر پروکسی پروتکل ندارد، به صورت خودکار socks5 اضافه می‌شود`  
  
        `let lines = [...new Set([combinedProxies.map](http://combinedProxies.map)(l =&gt; {`  
  
            `if (l.match(/^(socks4|socks5|socks|http|https|tg):\\/\\//i) || l.includes("t.me/socks")) {`  
  
                `return l;`  
  
            `}`  
  
            `return "socks5://" + l;`  
  
        `}))];`  
  
        `if (lines.length &gt; 0) {`  
  
            `for (let i = lines.length - 1; i &gt; 0; i--) {`  
  
                `const j = Math.floor(Math.random()  (i + 1));`  
  
                `[lines[i], lines[j]] = [lines[j], lines[i]];`  
  
            `}`  
  
            `let bestProxy = null;`  
  
            `let fallbackProxy = null;`  
  
            `const BATCH_SIZE = 5;`  
  
            `for (let i = 0; i &lt; lines.length; i += BATCH_SIZE) {`  
  
                `const batch = lines.slice(i, i + BATCH_SIZE);`  
  
                `loadingState.innerText = "تعداد " + lines.length + " پـروکـسـی پیدا شد درحال اسکن\\nاسکن گروه " + (Math.floor(i / BATCH_SIZE) + 1) + " (۵ تست برای هر کدام)...";`  
  
                `const testResults = await Promise.allSettled([batch.map](http://batch.map)(async (candidate) =&gt; {`  
  
                    `let successCount = 0;`  
  
                    `let totalPing = 0;`  
  
                    `let failCount = 0;`  
  
                    `for(let t = 0; t &lt; 5; t++) {`  
  
                        `const controller = new AbortController();`  
  
                        `const timeoutId = setTimeout(() =&gt; controller.abort(), 3500);`  
  
                        `try {`  
  
                            `const testRes = await fetch("/api/test-proxy", {`  
  
                                `method: "POST",`  
  
                                `headers: { "Content-Type": "application/json" },`  
  
                                `body: JSON.stringify({ proxy: candidate }),`  
  
                                `signal: controller.signal`  
  
                            `});`  
  
                            `clearTimeout(timeoutId);`  
  
                            `const testData = await testRes.json();`  
  
                            `if (testRes.ok &amp;&amp; testData.success) {`  
  
                                `successCount++;`  
  
                                `totalPing += [testData.ping](http://testData.ping);`  
  
                            `} else {`  
  
                                `failCount++;`  
  
                            `}`  
  
                        `} catch (err) {`  
  
                            `clearTimeout(timeoutId);`  
  
                            `failCount++;`  
  
                        `}`  
  
                        `if (failCount &gt; 2) break;`  
  
                    `}`  
  
                    `if (successCount &gt; 0) {`  
  
                        `return { proxy: candidate, successCount: successCount, avgPing: totalPing / successCount };`  
  
                    `}`  
  
                    `throw new Error();`  
  
                `}));`  
  
                `const successfulProxies = testResults`  
  
                    `.filter(r =&gt; r.status === "fulfilled")`  
  
                    `.map(r =&gt; r.value)`  
  
                    `.sort((a, b) =&gt; {`  
  
                        `if (b.successCount !== a.successCount) {`  
  
                            `return b.successCount - a.successCount;`  
  
                        `}`  
  
                        `return a.avgPing - b.avgPing;`  
  
                    `});`  
  
                `if (successfulProxies.length &gt; 0) {`  
  
                    `const topCandidate = successfulProxies[0];`  
  
                    `if (topCandidate.successCount &gt;= 3) {`  
  
                        `bestProxy = topCandidate.proxy;`  
  
                        `break;`  
  
                    `} else if (!fallbackProxy || topCandidate.successCount &gt; fallbackProxy.successCount) {`  
  
                        `fallbackProxy = topCandidate;`  
  
                    `}`  
  
                `}`  
  
            `}`  
  
            `if (!bestProxy &amp;&amp; fallbackProxy) {`  
  
                `bestProxy = fallbackProxy.proxy;`  
  
            `}`  
  
            `if (bestProxy) {`  
  
                `document.getElementById("user-socks5-input").value = bestProxy;`  
  
                `document.getElementById("test-user-proxy-result").innerText = "";`  
  
                `toggleProxySelectorModal(false);`  
  
                `showToast("پـروکـسـی با بهترین امتیاز لود شد.");`  
  
                `testUserSocksProxy();`  
  
            `} else {`  
  
                `alert("هیچ پـروکـسـی سالمی (حتی با یک پینگ موفق) یافت نشد.");`  
  
            `}`  
  
        `} else {`  
  
            `alert("پـروکـسـی برای این کشور یافت نشد.");`  
  
        `}`  
  
    `} catch (e) {`  
  
        `alert("خطا در دریافت لیست پـروکـسـی‌ها از سرور.");`  
  
    `} finally {`  
  
        `loadingState.classList.add("hidden");`  
  
        `formState.classList.remove("hidden");`  
  
        `fetchBtn.disabled = false;`  
  
    `}`  
  
`}`  
  
`const WORKER_DONATE_URL = "[https://si-491177.taile4bcbb.ts.net/donate](https://si-491177.taile4bcbb.ts.net/donate)";`  
  
		`function toggleDonateModal(show) {`  
  
			`setModalState('donate-modal', show);`  
  
			`if (!show) {`  
  
				`document.getElementById('donate-proxy-input').value = '';`  
  
				`const resultSpan = document.getElementById('donate-result');`  
  
				`if (resultSpan) {`  
  
					`resultSpan.innerText = '';`  
  
					`resultSpan.className = 'inline-block mt-1 text-[11px] font-bold transition-colors break-words leading-relaxed empty:hidden';`  
  
				`}`  
  
			`}`  
  
		`}`  
  
		`async function testAndDonateProxy() {`  
  
			`const proxyInput = document.getElementById('donate-proxy-input').value.trim();`  
  
			`const btn = document.getElementById('donate-submit-btn');`  
  
			`const resultSpan = document.getElementById('donate-result');`  
  
			`if (!proxyInput) {`  
  
				`resultSpan.innerText = 'لطفاً پـروکـسـی را وارد کنید!';`  
  
				`resultSpan.className = 'text-[11px] font-bold text-red-500 w-full mt-1';`  
  
				`return;`  
  
			`}`  
  
			`const strictProxyPattern = /^(?:(?:socks4|socks5|socks|http|https):\\/\\/)?([a-zA-Z0-9]{8}):([a-zA-Z0-9]{12})@([^:\\/]+):(\\d+)$/i;`  
  
			`if (!strictProxyPattern.test(proxyInput)) {`  
  
				`resultSpan.innerText = '❌ این پـروکـسـی اختصاصی نیست';`  
  
				`resultSpan.className = 'text-[11px] font-bold text-red-500 w-full mt-1 break-words';`  
  
				`return;`  
  
			`}`  
  
			`btn.disabled = true;`  
  
			`btn.innerText = 'صبر کنید...';`  
  
			`resultSpan.innerText = 'در حال تست با اسکنر پـنـل...';`  
  
			`resultSpan.className = 'text-[11px] font-bold text-emerald-500 w-full mt-1';`  
  
			`const controller = new AbortController();`  
  
			`const timeoutId = setTimeout(() =&gt; controller.abort(), 6000);`  
  
			`try {`  
  
				`const testRes = await fetch('/api/test-proxy', {`  
  
					`method: 'POST',`  
  
					`headers: { 'Content-Type': 'application/json' },`  
  
					`body: JSON.stringify({ proxy: proxyInput }),`  
  
					`signal: controller.signal`  
  
				`});`  
  
				`clearTimeout(timeoutId);`  
  
				`const testData = await testRes.json();`  
  
				`if (!testRes.ok || !testData.success) {`  
  
					`throw new Error(testData.error || 'پـروکـسـی مسدود یا خاموش است');`  
  
				`}`  
  
				`const countryCode = [testData.country](http://testData.country) || 'UN';`  
  
				`resultSpan.innerText = 'پـروکـسـی سالم است! در حال ارسال (' + countryCode + ')...';`  
  
				`const donateResponse = await fetch(WORKER_DONATE_URL, {`  
  
					`method: 'POST',`  
  
					`headers: { 'Content-Type': 'application/json' },`  
  
					`body: JSON.stringify({`  
  
						`proxy: proxyInput,`  
  
						`country: countryCode`  
  
					`})`  
  
				`});`  
  
				`const donateData = await donateResponse.json();`  
  
				`if (donateData.success) {`  
  
					`resultSpan.innerText = '✅ ' + donateData.message;`  
  
					`resultSpan.className = 'text-[11px] font-bold text-green-600 w-full mt-1';`  
  
					`document.getElementById('donate-proxy-input').value = '';`  
  
				`} else {`  
  
					`resultSpan.innerText = '❌ خطا: ' + donateData.error;`  
  
					`resultSpan.className = 'text-[11px] font-bold text-red-500 w-full mt-1 break-words';`  
  
				`}`  
  
			`} catch (error) {`  
  
				`clearTimeout(timeoutId);`  
  
				`let errorMsg = error.message;`  
  
				`if ([error.name](http://error.name) === 'AbortError') errorMsg = 'تایم‌اوت در تست پـروکـسـی';`  
  
				`resultSpan.innerText = '❌ خطا: ' + errorMsg;`  
  
				`resultSpan.className = 'text-[11px] font-bold text-red-500 w-full mt-1 break-words';`  
  
			`} finally {`  
  
				`btn.disabled = false;`  
  
				`btn.innerText = 'تست و اهدا';`  
  
			`}`  
  
		`}`  
  
		`function toggleSupportModal(show) {`  
  
            `const modal = document.getElementById('support-modal');`  
  
            `const content = modal.firstElementChild;`  
  
            `if (show) {`  
  
                `modal.classList.remove('opacity-0', 'pointer-events-none');`  
  
                `content.classList.remove('opacity-0', 'scale-95');`  
  
            `} else {`  
  
                `modal.classList.add('opacity-0', 'pointer-events-none');`  
  
                `content.classList.add('opacity-0', 'scale-95');`  
  
            `}`  
  
        `}`  
  
`window.addEventListener('click', (e) =&gt; {`  
  
    `if (window._modalMouseDownTarget &amp;&amp; window._modalMouseDownTarget !== [e.target](http://e.target)) return;`  
  
    `if ([e.target.id](http://e.target.id) === 'proxy-selector-modal') toggleProxySelectorModal(false);`  
  
	`if ([e.target.id](http://e.target.id) === 'donate-modal') toggleDonateModal(false);`  
  
`});`  
  
    `&lt;/script&gt;`  
  
`&lt;/body&gt;`  
  
`&lt;/html&gt;`*,*  
  
	*status:* `&lt;!DOCTYPE html&gt;`  
  
`&lt;html lang="fa" dir="rtl" class="dark"&gt;`  
  
`&lt;head&gt;`  
  
    `&lt;meta charset="UTF-8"&gt;`  
  
    `&lt;meta name="viewport" content="width=device-width, initial-scale=1.0"&gt;`  
  
    `&lt;title&gt;وضعیت اشتراک کاربر&lt;/title&gt;`  
  
    `&lt;script src="[https://cdn.tailwindcss.com"&gt;&lt;/script&gt;](https://cdn.tailwindcss.com"></script>)`  
  
    `&lt;link href="[https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css](https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css)" rel="stylesheet" type="text/css" /&gt;`  
  
    `&lt;div id="toast-container" class="fixed bottom-5 left-1/2 -translate-x-1/2 z-[9999] flex flex-col gap-2 pointer-events-none"&gt;&lt;/div&gt;`  
  
    ``  
  
    `&lt;script&gt;`  
  
        `// showToast و tailwind.config (همان کد اول)`  
  
        `function showToast(message) {`  
  
            `const container = document.getElementById('toast-container');`  
  
            `const toast = document.createElement('div');`  
  
            `toast.className = 'px-5 py-3 bg-amber-500/90 backdrop-blur-md border border-amber-400/50 text-white rounded-full shadow-[0_4px_20px_rgba(245,158,11,0.4)] font-bold text-xs transform transition-all duration-500 translate-y-full opacity-0 whitespace-nowrap';`  
  
            `toast.innerText = message;`  
  
            `container.appendChild(toast);`  
  
            `requestAnimationFrame(() =&gt; {`  
  
                `toast.classList.remove('translate-y-full', 'opacity-0');`  
  
            `});`  
  
            `setTimeout(() =&gt; {`  
  
                `toast.classList.add('translate-y-full', 'opacity-0');`  
  
                `setTimeout(() =&gt; toast.remove(), 500);`  
  
            `}, 3000);`  
  
        `}`  
  
        `tailwind.config = {`  
  
            `darkMode: 'class',`  
  
            `theme: {`  
  
                `extend: {`  
  
                    `fontFamily: { sans: ['Vazirmatn', 'sans-serif'] },`  
  
                    `colors: { amoled: { bg: '#100904ff', card: '#120b08', input: '#19110d', border: '#2d1e16' } }`  
  
                `}`  
  
            `}`  
  
        `}`  
  
    `&lt;/script&gt;`  
  
    `&lt;style&gt;`  
  
        `body { font-family: 'Vazirmatn', sans-serif; transition: all 0.4s ease; }`  
  
        `.glass { backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px); background: rgba(255, 255, 255, 0.05); border: 1px solid rgba(255, 255, 255, 0.08); }`  
  
        `.dark .glass { background: rgba(0, 0, 0, 0.4); border-color: rgba(255, 255, 255, 0.06); }`  
  
        `.glow-gold { box-shadow: 0 0 30px rgba(245, 158, 11, 0.15); }`  
  
        `.stat-card { transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); }`  
  
        `.stat-card:hover { transform: translateY(-4px) scale(1.01); }`  
  
        `.progress-ring { transition: stroke-dashoffset 0.8s ease; }`  
  
        `.badge-pulse { animation: pulse 2s infinite; }`  
  
        `@keyframes pulse { 0% { opacity: 0.6; } 50% { opacity: 1; } 100% { opacity: 0.6; } }`  
  
        `.toast-container { position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%); z-index: 9999; display: flex; flex-direction: column; gap: 8px; pointer-events: none; }`  
  
        `.toast { padding: 12px 24px; border-radius: 40px; font-weight: bold; font-size: 0.9rem; backdrop-filter: blur(12px); background: rgba(245, 158, 11, 0.9); color: #fff; box-shadow: 0 8px 30px rgba(245, 158, 11, 0.3); border: 1px solid rgba(255, 255, 255, 0.15); transform: translateY(20px); opacity: 0; transition: all 0.5s ease; pointer-events: auto; }`  
  
        `.[toast.show](http://toast.show) { transform: translateY(0); opacity: 1; }`  
  
    `&lt;/style&gt;`  
  
`&lt;/head&gt;`  
  
`&lt;body class="min-h-screen bg-gradient-to-br from-amber-50 to-orange-50 dark:from-[#0a0806] dark:to-[#1a120e] text-stone-800 dark:text-zinc-100 flex flex-col items-center justify-between p-4 md:p-8 font-sans antialiased selection:bg-amber-200 dark:selection:bg-zinc-800 transition-colors duration-300" id="main-body"&gt;`  
  
    `&lt;div class="w-full max-w-2xl glass rounded-3xl shadow-2xl p-6 md:p-8 relative overflow-hidden glow-gold"&gt;`  
  
        `&lt;!-- decorative blobs --&gt;`  
  
        `&lt;div class="absolute -top-20 -left-20 w-64 h-64 bg-amber-500/10 rounded-full blur-3xl pointer-events-none"&gt;&lt;/div&gt;`  
  
        `&lt;div class="absolute -bottom-20 -right-20 w-64 h-64 bg-orange-500/10 rounded-full blur-3xl pointer-events-none"&gt;&lt;/div&gt;`  
  
`&lt;!-- آیکون ماهواره (حرفه‌ای و با جزئیات) --&gt;`  
  
`&lt;div class="flex justify-center mb-2.5"&gt;`  
  
    `&lt;svg class="w-12 h-12 md:w-16 md:h-16 text-amber-500 dark:text-amber-400 sunset:text-amber-400 drop-shadow-lg" `  
  
         `fill="none" `  
  
         `stroke="currentColor" `  
  
         `viewBox="0 0 24 24" `  
  
         `stroke-width="1.5" `  
  
         `stroke-linecap="round" `  
  
         `stroke-linejoin="round"&gt;`  
  
        ``  
  
        `&lt;!-- بدنه اصلی ماهواره (مکعب با گوشه‌های گرد) --&gt;`  
  
        `&lt;rect x="8" y="8" width="8" height="8" rx="1.5" /&gt;`  
  
        ``  
  
        `&lt;!-- آنتن اصلی (بالای ماهواره) --&gt;`  
  
        `&lt;path d="M12 6v2" /&gt;`  
  
        `&lt;circle cx="12" cy="5" r="0.8" fill="currentColor" /&gt;`  
  
        ``  
  
        `&lt;!-- آنتن فرعی (کناری) --&gt;`  
  
        `&lt;path d="M16 12h2" /&gt;`  
  
        `&lt;circle cx="18.5" cy="12" r="0.8" fill="currentColor" /&gt;`  
  
        ``  
  
        `&lt;!-- پنل خورشیدی چپ --&gt;`  
  
        `&lt;rect x="4" y="10" width="4" height="4" rx="0.5" /&gt;`  
  
        `&lt;line x1="5" y1="11" x2="7" y2="11" stroke-width="0.5" /&gt;`  
  
        `&lt;line x1="5" y1="12" x2="7" y2="12" stroke-width="0.5" /&gt;`  
  
        `&lt;line x1="5" y1="13" x2="7" y2="13" stroke-width="0.5" /&gt;`  
  
        ``  
  
        `&lt;!-- پنل خورشیدی راست --&gt;`  
  
        `&lt;rect x="16" y="10" width="4" height="4" rx="0.5" /&gt;`  
  
        `&lt;line x1="17" y1="11" x2="19" y2="11" stroke-width="0.5" /&gt;`  
  
        `&lt;line x1="17" y1="12" x2="19" y2="12" stroke-width="0.5" /&gt;`  
  
        `&lt;line x1="17" y1="13" x2="19" y2="13" stroke-width="0.5" /&gt;`  
  
        ``  
  
        `&lt;!-- آنتن دیش (زیر ماهواره) --&gt;`  
  
        `&lt;path d="M12 16v2" /&gt;`  
  
        `&lt;path d="M10 18l2 2 2-2" /&gt;`  
  
        ``  
  
        `&lt;!-- نقاط نورانی (LED indicators) روی بدنه --&gt;`  
  
        `&lt;circle cx="10.5" cy="10.5" r="0.5" fill="currentColor" opacity="0.7" /&gt;`  
  
        `&lt;circle cx="13.5" cy="10.5" r="0.5" fill="currentColor" opacity="0.7" /&gt;`  
  
        `&lt;circle cx="10.5" cy="13.5" r="0.5" fill="currentColor" opacity="0.7" /&gt;`  
  
        `&lt;circle cx="13.5" cy="13.5" r="0.5" fill="currentColor" opacity="0.7" /&gt;`  
  
    `&lt;/svg&gt;`  
  
`&lt;/div&gt;`  
  
        `&lt;!-- header --&gt;`  
  
        `&lt;div class="text-center mb-8 relative z-10"&gt;`  
  
                `&lt;h1 class="text-2xl font-bold tracking-tight text-amber-950 dark:text-white mb-1"&gt;`  
  
        `&lt;span class="text-white dark:text-white"&gt;اشتراک &lt;/span&gt;`  
  
        `&lt;span class="text-amber-500 dark:text-amber-400 sunset:text-amber-400"&gt;ما&lt;/span&gt;`  
  
        `&lt;span class="text-white dark:text-white"&gt;که&lt;/span&gt;`  
  
        `&lt;span class="text-amber-500 dark:text-amber-400 sunset:text-amber-400"&gt;وصلیم🚀&lt;/span&gt;`  
  
    `&lt;/h1&gt;`  
  
                `&lt;div class="inline-block px-3 py-1.5 mt-1 border border-amber-500/30 dark:border-amber-400/30 rounded-xl bg-white/10 dark:bg-zinc-900/30 backdrop-blur-sm shadow-sm"&gt;`  
  
        `&lt;p class="text-sm md:text-base font-mono font-semibold text-amber-600 dark:text-amber-400 tracking-wide" id="display-username"&gt;&lt;/p&gt;`  
  
    `&lt;/div&gt;`  
  
            `&lt;div id="live-connections-badge" class="hidden inline-flex items-center gap-2.5 px-4 py-1.5 mt-3 bg-amber-500/10 border-2 border-amber-500/30 text-amber-500 rounded-full text-xs font-bold shadow-md backdrop-blur-sm transition-all duration-300 hover:scale-105"&gt;`  
  
                `&lt;span class="w-2 h-2 rounded-full bg-amber-500 animate-pulse"&gt;&lt;/span&gt;`  
  
                `&lt;span id="live-connections-text"&gt;۰ دستگاه متصل&lt;/span&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
        `&lt;!-- stats grid - only 3 cards: volume, expiry, online --&gt;`  
  
        `&lt;div class="grid grid-cols-1 md:grid-cols-3 gap-5 mb-8 relative z-10"&gt;`  
  
            `&lt;!-- volume card --&gt;`  
  
            `&lt;div class="stat-card bg-white/60 dark:bg-zinc-900/40 border border-amber-200/50 dark:border-amber-800/20 rounded-2xl p-5 shadow-sm hover:shadow-md backdrop-blur-sm"&gt;`  
  
                `&lt;div class="flex items-center justify-between mb-2"&gt;`  
  
                    `&lt;span class="text-xs font-semibold text-amber-800 dark:text-amber-300 flex items-center gap-1.5"&gt;`  
  
                                    `&lt;svg class="w-4 h-4 text-amber-500" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"&gt;`  
  
                `&lt;ellipse cx="12" cy="5" rx="9" ry="3" /&gt;`  
  
                `&lt;path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" /&gt;`  
  
                `&lt;path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" /&gt;`  
  
            `&lt;/svg&gt;`  
  
                        `حجم 📊`  
  
                    `&lt;/span&gt;`  
  
                    `&lt;span id="volume-pct" class="text-xs font-bold text-amber-600 dark:text-amber-400"&gt;۰٪&lt;/span&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;div class="w-full bg-amber-200/50 dark:bg-amber-950/40 rounded-full h-2.5 overflow-hidden mb-3"&gt;`  
  
                    `&lt;div id="volume-progress" class="bg-gradient-to-r from-amber-500 to-orange-500 h-2.5 rounded-full transition-all duration-1000" style="width: 0%"&gt;&lt;/div&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;div class="flex justify-between text-xs text-amber-800 dark:text-amber-300 font-medium"&gt;`  
  
                    `&lt;span&gt;مصرف شده: &lt;span id="used-vol" class="font-bold text-amber-950 dark:text-amber-100"&gt;-&lt;/span&gt;&lt;/span&gt;`  
  
                    `&lt;span&gt;حجم کل: &lt;span id="limit-vol" class="font-bold text-amber-950 dark:text-amber-100"&gt;-&lt;/span&gt;&lt;/span&gt;`  
  
                `&lt;/div&gt;`  
  
            `&lt;/div&gt;`  
  
            `&lt;!-- expiry card --&gt;`  
  
            `&lt;div class="stat-card bg-white/60 dark:bg-zinc-900/40 border border-amber-200/50 dark:border-amber-800/20 rounded-2xl p-5 shadow-sm hover:shadow-md backdrop-blur-sm"&gt;`  
  
                `&lt;div class="flex items-center justify-between mb-2"&gt;`  
  
                    `&lt;span class="text-xs font-semibold text-amber-800 dark:text-amber-300 flex items-center gap-1.5"&gt;`  
  
                        `&lt;svg class="w-4 h-4 text-amber-500" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"&gt;`  
  
    `&lt;rect x="3" y="4" width="18" height="18" rx="2" ry="2" /&gt;`  
  
    `&lt;line x1="16" y1="2" x2="16" y2="6" /&gt;`  
  
    `&lt;line x1="8" y1="2" x2="8" y2="6" /&gt;`  
  
    `&lt;line x1="3" y1="10" x2="21" y2="10" /&gt;`  
  
`&lt;/svg&gt;`  
  
                        `زمان 📊`  
  
                    `&lt;/span&gt;`  
  
                    `&lt;span id="expiry-pct" class="text-xs font-bold text-amber-600 dark:text-amber-400"&gt;۰٪&lt;/span&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;div class="w-full bg-amber-200/50 dark:bg-amber-950/40 rounded-full h-2.5 overflow-hidden mb-3"&gt;`  
  
                    `&lt;div id="expiry-progress" class="bg-gradient-to-r from-amber-400 to-amber-600 h-2.5 rounded-full transition-all duration-1000" style="width: 0%"&gt;&lt;/div&gt;`  
  
                `&lt;/div&gt;`  
  
                `&lt;div class="flex justify-between text-xs text-amber-800 dark:text-amber-300 font-medium"&gt;`  
  
                    `&lt;span&gt;باقی‌مانده: &lt;span id="days-remaining" class="font-bold text-amber-950 dark:text-amber-100"&gt;-&lt;/span&gt;&lt;/span&gt;`  
  
                    `&lt;span&gt;کل اعتبار: &lt;span id="total-days" class="font-bold text-amber-950 dark:text-amber-100"&gt;-&lt;/span&gt;&lt;/span&gt;`  
  
                `&lt;/div&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;!-- action buttons --&gt;`  
  
        `&lt;div class="border-t border-amber-200/30 dark:border-zinc-800/50 pt-6 relative z-10"&gt;`  
  
            `&lt;h2 class="text-sm font-bold mb-4 flex items-center gap-2 text-amber-800 dark:text-amber-300"&gt;`  
  
                `&lt;svg class="w-4 h-4 text-amber-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"&gt;&lt;path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/&gt;&lt;/svg&gt;`  
  
                `دریافت کانفیگ ها و ساب متنی`  
  
            `&lt;/h2&gt;`  
  
            `&lt;div class="space-y-3"&gt;`  
  
                `&lt;button onclick="copyTextSub()" class="w-full flex justify-between items-center px-5 py-3.5 bg-white/70 dark:bg-zinc-900/50 border border-amber-200/50 dark:border-amber-800/20 hover:border-amber-500 dark:hover:border-amber-500 rounded-xl text-sm font-medium transition-all duration-200 shadow-sm hover:shadow-md backdrop-blur-sm group"&gt;`  
  
                    `&lt;span class="flex items-center gap-2 text-amber-800 dark:text-amber-300 group-hover:text-amber-600 dark:group-hover:text-amber-400"&gt;⚡ کپی لینک ساب متنی برای نرم افزار&lt;/span&gt;`  
  
                    `&lt;span class="text-amber-500 text-xs font-bold"&gt;کپی&lt;/span&gt;`  
  
                `&lt;/button&gt;`  
  
                `&lt;button onclick="copyVlessConfig()" class="w-full flex justify-between items-center px-5 py-3.5 bg-white/70 dark:bg-zinc-900/50 border border-amber-200/50 dark:border-amber-800/20 hover:border-orange-500 dark:hover:border-orange-500 rounded-xl text-sm font-medium transition-all duration-200 shadow-sm hover:shadow-md backdrop-blur-sm group"&gt;`  
  
                    `&lt;span class="flex items-center gap-2 text-amber-800 dark:text-amber-300 group-hover:text-orange-600 dark:group-hover:text-orange-400"&gt;🛰️ کپی تمامی کانفیگ ها (مستقیم)&lt;/span&gt;`  
  
                    `&lt;span class="text-orange-500 text-xs font-bold"&gt;کپی&lt;/span&gt;`  
  
                `&lt;/button&gt;`  
  
            `&lt;/div&gt;`  
  
        `&lt;/div&gt;`  
  
    `&lt;/div&gt;`  
  
`&lt;/div&gt;`  
  
    `&lt;!-- footer --&gt;`  
  
    `&lt;div class="flex items-center justify-center gap-4 mt-6 z-10 flex-wrap"&gt;`  
  
        `&lt;a href="[https://t.me/makevaslim4](https://t.me/makevaslim4)" target="_blank" class="flex items-center gap-2 px-4 py-2 bg-white/70 dark:bg-zinc-900/50 border border-amber-200/50 dark:border-amber-800/20 rounded-full shadow-sm hover:shadow-md transition text-sm font-bold text-amber-800 dark:text-amber-300 hover:text-amber-600 dark:hover:text-amber-400 group backdrop-blur-sm"&gt;`  
  
            `&lt;svg class="w-5 h-5 text-amber-500 group-hover:scale-110 transition" viewBox="0 0 24 24" fill="currentColor"&gt;&lt;path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-1.13 7.19-.14.75-.42 1-.68 1.03-.58.05-1.02-.38-1.58-.75-.88-.58-1.38-.94-2.23-1.5-.99-.65-.35-1.01.22-1.59.15-.15 2.71-2.48 2.76-2.69a.2.2 0 00-.05-.18c-.06-.05-.14-.03-.21-.02-.09.02-1.49.94-4.22 2.79-.4.27-.76.41-1.08.4-.36-.01-1.04-.2-1.55-.37-.63-.2-1.12-.31-1.08-.66.02-.18.27-.36.74-.55 2.92-1.27 4.86-2.11 5.83-2.51 2.78-1.16 3.35-1.36 3.73-1.37.08 0 .27.02.39.12.1.08.13.19.14.27-.01.06.01.24 0 .24z"/&gt;&lt;/svg&gt;`  
  
            `Makevaslim`  
  
        `&lt;/a&gt;`  
  
    `&lt;/div&gt;`  
  
    `&lt;div class="text-xs text-zinc-500 dark:text-zinc-500 mt-5 text-center border-t border-zinc-900/10 dark:border-zinc-800/50 pt-4 opacity-80 select-none"&gt;`  
  
        `طراحی شده توسط تیم `  
  
        `&lt;span class="inline-block align-middle mx-1 font-bold text-amber-700 dark:text-amber-400"&gt;ماکه‌وصلیم&lt;/span&gt;`  
  
        `&lt;span class="inline-block align-middle mx-1 text-amber-500"&gt;🖤19🖤18🖤&lt;/span&gt;`  
  
    `&lt;/div&gt;`  
  
    `&lt;!-- toast container --&gt;`  
  
    `&lt;div class="toast-container" id="toast-container"&gt;&lt;/div&gt;`  
  
    `&lt;script&gt;`  
  
        `/ {{USER_DATA_PLACEHOLDER}} /`  
  
        `function getHost() {`  
  
            `return [window.location.host](http://window.location.host);`  
  
        `}`  
  
        `function getVlessLink() {`  
  
            `const u = window.statusUser;`  
  
            `const host = getHost();`  
  
            `var ips = [host];`  
  
            `if (u.ips) {`  
  
                `ips = u.ips.split('\\n').map(function(ip) { return ip.trim(); }).filter(function(ip) { return ip.length &gt; 0; });`  
  
                `if (ips.length === 0) ips = [host];`  
  
            `}`  
  
            `var ports = String(u.port || '443').split(',').map(function(p) { return p.trim(); }).filter(function(p) { return p.length &gt; 0; });`  
  
            `var fp = u.fingerprint || 'chrome';`  
  
            `var links = [];`  
  
            `ips.forEach(function(ip, ipIndex) {`  
  
                `ports.forEach(function(portStr) {`  
  
                    `var isTlsPort = ['443', '2053', '2083', '2087', '2096', '8443'].includes(portStr);`  
  
                    `var tlsVal = isTlsPort ? 'tls' : 'none';`  
  
                    `var remark = ips.length &gt; 1 ? (u.username + '-' + (ipIndex + 1) + '-' + portStr) : (u.username + '-' + portStr);`  
  
                    `links.push('vle' + 'ss://' + (u.uuid || '') + '@' + ip + ':' + portStr + '?path=%2FMa_Ke_Vaslim&amp;security=' + tlsVal + '&amp;encryption=none&amp;insecure=0&amp;host=' + host + '&amp;fp=' + fp + '&amp;type=ws&amp;allowInsecure=0&amp;sni=' + host + '#' + encodeURIComponent(remark));`  
  
                `});`  
  
            `});`  
  
            `return links.join('\\n');`  
  
        `}`  
  
        `// جایگزینی کدهای قدیمی کپی ساب‌اسکریپشن و VLESS`  
  
`async function copyVlessConfig() {`  
  
    `try {`  
  
        `const subUrl = window.location.protocol + '//' + getHost() + '/sub/' + encodeURIComponent(window.statusUser.username);`  
  
        `const response = await fetch(subUrl);`  
  
        `const allConfigs = await response.text();`  
  
        `await navigator.clipboard.writeText(allConfigs);`  
  
        `// استفاده از Toast جدید به جای alert`  
  
        `showToast('✅ همه کانفیگ‌ها با موفقیت کپی شدند، ماکه‌وصلیم.');`  
  
    `} catch (error) {`  
  
        `showToast('❌ خطا در دریافت کانفیگ‌ها از سرور');`  
  
        `console.error(error);`  
  
    `}`  
  
`}`  
  
`function copyTextSub() {`  
  
    `const link = window.location.protocol + '//' + getHost() + '/sub/' + encodeURIComponent(window.statusUser.username);`  
  
    `navigator.clipboard.writeText(link).then(() =&gt; {`  
  
        `// استفاده از Toast جدید به جای alert`  
  
        `showToast('✅ لینک ساب متنی کپی شد، ماکه‌وصلیم.');`  
  
    `}).catch(() =&gt; {`  
  
        `showToast('❌ خطا در کپی لینک ساب!');`  
  
    `});`  
  
`}`  
  
        `document.addEventListener('DOMContentLoaded', () =&gt; {`  
  
            `const u = window.statusUser;`  
  
            `if (!u) return;`  
  
            `document.getElementById('display-username').innerText = u.username;`  
  
            `const badge = document.getElementById('live-connections-badge');`  
  
            `badge.classList.remove('hidden');`  
  
            `if ([u.online](http://u.online)_count &amp;&amp; [u.online](http://u.online)_count &gt; 0) {`  
  
                `document.getElementById('live-connections-text').innerText = [u.online](http://u.online)_count + (u.max_connections ? '/' + u.max_connections : '') + ' دستگاه متصل';`  
  
                `badge.className = 'inline-flex items-center gap-1.5 px-3 py-1 bg-amber-500/10 border border-amber-500/20 text-amber-500 rounded-full text-xs font-bold shadow-sm';`  
  
                `badge.querySelector('span.w-2').className = 'w-2 h-2 rounded-full bg-amber-500 animate-pulse';`  
  
            `} else {`  
  
                `document.getElementById('live-connections-text').innerText = '۰ دستگاه متصل';`  
  
                `badge.className = 'inline-flex items-center gap-1.5 px-3 py-1 bg-gray-500/10 border border-gray-500/20 text-gray-500 dark:text-zinc-400 rounded-full text-xs font-bold shadow-sm';`  
  
                `badge.querySelector('span.w-2').className = 'w-2 h-2 rounded-full bg-gray-500';`  
  
            `// بارگذاری خودکار و تبدیل آنی به فرمت متنی Vless درون کادر`  
  
`const subUrlText = window.location.protocol + '//' + getHost() + '/sub/' + encodeURIComponent(u.username);`  
  
`fetch(subUrlText)`  
  
    `.then(res =&gt; res.text())`  
  
    `.then(rawData =&gt; {`  
  
        `// تبدیل خودکار خروجی سرور به لینک‌های Vless`  
  
        `document.getElementById('configs-text-area').value = decodeSubToVless(rawData);`  
  
    `})`  
  
    `.catch(err =&gt; {`  
  
        `document.getElementById('configs-text-area').value = '❌ خطا در بارگذاری متنی کانفیگ‌ها از سرور';`  
  
        `console.error(err);`  
  
    `});`  
  
    ``  
  
                `}`  
  
            `// Compute volume`  
  
            `const usedGb = u.used_gb || 0;`  
  
            `const limitGb = u.limit_gb;`  
  
            `const formattedUsed = usedGb &lt; 1 ? (usedGb  1024).toFixed(0) + ' MB' : usedGb.toFixed(2) + ' GB';`  
  
            `document.getElementById('used-vol').innerText = formattedUsed;`  
  
            `let isVolumeExpired = false;`  
  
            `if (limitGb) {`  
  
                `document.getElementById('limit-vol').innerText = limitGb + ' GB';`  
  
                `const pct = Math.min((usedGb / limitGb)  100, 100);`  
  
                `document.getElementById('volume-pct').innerText = pct.toFixed(0) + '٪';`  
  
                `document.getElementById('volume-progress').style.width = pct + '%';`  
  
                `// Color bar`  
  
                `const hue = 35 - (pct  0.35);`  
  
                `document.getElementById('volume-progress').style.backgroundColor = 'hsl(' + hue + ', 85%, 45%)';`  
  
                `if (usedGb &gt;= limitGb) isVolumeExpired = true;`  
  
            `} else {`  
  
                `document.getElementById('limit-vol').innerText = 'نامحدود';`  
  
                `document.getElementById('volume-pct').innerText = '۰٪';`  
  
                `document.getElementById('volume-progress').style.width = '100%';`  
  
                `document.getElementById('volume-progress').style.backgroundColor = '#f97316';`  
  
            `}`  
  
            `// Compute Expiry`  
  
            `let daysRemaining = 'نامحدود';`  
  
            `let totalDays = 'نامحدود';`  
  
            `let isTimeExpired = false;`  
  
            `if (u.expiry_days) {`  
  
                `totalDays = u.expiry_days + ' روز';`  
  
                `if (u.created_at) {`  
  
                    `const created = new Date(u.created_at);`  
  
                    `const expiryDate = new Date(created.getTime() + (u.expiry_days  24  60  60  1000));`  
  
                    `const diffDays = Math.ceil((expiryDate - new Date()) / (1000  60  60  24));`  
  
                    `daysRemaining = diffDays &gt; 0 ? diffDays : 0;`  
  
                    `const pct = Math.max(0, Math.min(100, (daysRemaining / u.expiry_days)  100));`  
  
                    `document.getElementById('expiry-pct').innerText = pct.toFixed(0) + '٪';`  
  
                    `document.getElementById('expiry-progress').style.width = pct + '%';`  
  
                    `const hue = pct  0.35;`  
  
                    `document.getElementById('expiry-progress').style.backgroundColor = 'hsl(' + hue + ', 85%, 45%)';`  
  
                    `if (new Date() &gt; expiryDate) isTimeExpired = true;`  
  
                `}`  
  
            `} else {`  
  
                `document.getElementById('expiry-pct').innerText = '۰٪';`  
  
                `document.getElementById('expiry-progress').style.width = '100%';`  
  
                `document.getElementById('expiry-progress').style.backgroundColor = '#eab308';`  
  
            `}`  
  
            `document.getElementById('days-remaining').innerText = daysRemaining === 'نامحدود' ? 'نامحدود' : daysRemaining + ' روز';`  
  
            `document.getElementById('total-days').innerText = totalDays;`  
  
            `const usedReq = u.used_req || 0;`  
  
            `const limitReq = u.limit_req;`  
  
            `document.getElementById('used-req').innerText = usedReq.toLocaleString();`  
  
            `let isReqExpired = false;`  
  
            `if (limitReq) {`  
  
                `document.getElementById('limit-req').innerText = limitReq.toLocaleString();`  
  
                `const rPct = Math.min((usedReq / limitReq)  100, 100);`  
  
                `document.getElementById('req-pct').innerText = rPct.toFixed(0) + '٪';`  
  
                `document.getElementById('req-progress').style.width = rPct + '%';`  
  
                `const rHue = 35 - (rPct  0.35);`  
  
                `document.getElementById('req-progress').style.backgroundColor = 'hsl(' + rHue + ', 85%, 45%)';`  
  
                `if (usedReq &gt;= limitReq) isReqExpired = true;`  
  
            `} else {`  
  
                `document.getElementById('limit-req').innerText = 'نامحدود';`  
  
                `document.getElementById('req-pct').innerText = '۰٪';`  
  
                `document.getElementById('req-progress').style.width = '100%';`  
  
                `document.getElementById('req-progress').style.backgroundColor = '#f97316';`  
  
            `}`  
  
            `const onlineCount = [u.online](http://u.online)_count || 0;`  
  
            `const maxConns = u.max_connections;`  
  
            `document.getElementById('online-count').innerText = onlineCount;`  
  
            `if (maxConns) {`  
  
                `document.getElementById('limit-online').innerText = maxConns;`  
  
                `const oPct = Math.min((onlineCount / maxConns)  100, 100);`  
  
                `document.getElementById('online-pct').innerText = oPct.toFixed(0) + '٪';`  
  
                `document.getElementById('online-progress').style.width = oPct + '%';`  
  
                `const oHue = 35 - (oPct * 0.35);`  
  
                `document.getElementById('online-progress').style.backgroundColor = 'hsl(' + oHue + ', 85%, 45%)';`  
  
            `} else {`  
  
                `document.getElementById('limit-online').innerText = 'نامحدود';`  
  
                `document.getElementById('online-pct').innerText = '۰٪';`  
  
                `document.getElementById('online-progress').style.width = '100%';`  
  
                `document.getElementById('online-progress').style.backgroundColor = onlineCount &gt; 0 ? '#ea580c' : '#78716c'; `  
  
            `}`  
  
            `const statusCard = document.getElementById('status-card');`  
  
            `const statusText = document.getElementById('status-text');`  
  
            `if ([u.is](http://u.is)_active === 0) {`  
  
                `statusCard.className = 'mb-6 rounded-2xl p-4 text-center border font-bold relative z-10 bg-red-500/10 border-red-500/30 text-red-500 shadow-md shadow-red-500/5';`  
  
                `[statusCard.style](http://statusCard.style).boxShadow = 'inset 0 0 12px rgba(239, 68, 68, 0.1)';`  
  
                `statusText.innerText = '❌ وضعیت اشتراک: غیرفعال / مسدود دستی';`  
  
            `} else if (isVolumeExpired) {`  
  
                `statusCard.className = 'mb-6 rounded-2xl p-4 text-center border font-bold relative z-10 bg-orange-500/10 border-orange-500/30 text-orange-500 shadow-md shadow-orange-500/5';`  
  
                `statusText.innerText = '⚠️ وضعیت اشتراک: تمام شدن حجم مجاز';`  
  
            `} else if (isReqExpired) {`  
  
                `statusCard.className = 'mb-6 rounded-2xl p-4 text-center border font-bold relative z-10 bg-orange-500/10 border-orange-500/30 text-orange-500 shadow-md shadow-orange-500/5';`  
  
                `statusText.innerText = '⚠️ وضعیت اشتراک: تمام شدن ریکوئست مجاز';`  
  
            `} else if (isTimeExpired) {`  
  
                `statusCard.className = 'mb-6 rounded-2xl p-4 text-center border font-bold relative z-10 bg-orange-500/10 border-orange-500/30 text-orange-500 shadow-md shadow-orange-500/5';`  
  
                `statusText.innerText = '⏳ وضعیت اشتراک: منقضی شده (پایان زمان اعتبار)';`  
  
            `} else {`  
  
                `statusCard.className = 'mb-6 rounded-2xl p-4 text-center border font-bold relative z-10 bg-amber-500/10 border-amber-500/30 text-amber-500 shadow-md shadow-amber-500/5';`  
  
                `statusText.innerText = '✅ وضعیت اشتراک: فعال و متصل';`  
  
            `}`  
  
        `});`  
  
`// تابع اختصاصی تغییر تم داینامیک فایل اول`  
  
        `function setTheme(themeName) {`  
  
    `const body = document.getElementById('main-body');`  
  
    `if (!body) return;`  
  
    ``  
  
    `// حذف تم‌های قبلی (کلمه 'theme-day' حذف شد)`  
  
    `body.classList.remove('theme-sunset', 'theme-night', 'bg-amber-50', 'dark:bg-amoled-bg', 'text-stone-800', 'dark:text-zinc-100');`  
  
    `body.classList.add('theme-' + themeName);`  
  
    ``  
  
    `// مدیریت رنگ نوشته‌ها متناسب با هر تم (شرط مربوط به day حذف شد)`  
  
    `if (themeName === 'night') {`  
  
        `[body.style](http://body.style).color = '#93c5fd';`  
  
    `} else {`  
  
        `[body.style](http://body.style).color = '#fbd38d';`  
  
    `}`  
  
`}`  
  
    `&lt;/script&gt;`  
  
`&lt;/body&gt;`  
  
`&lt;/html&gt;`,  
  
};   
  
همه کد های ریپازیتوریمون رو دقیقا عین کد های همینجا که برات نوشتم بکن

دقیق اینکارو انجام بده و هیچ کاری به باگ ها نداشته باشه و فقط کاری که گفتم رو انجام بده
