import { connect } from "cloudflare:sockets";
const GLOBAL_TRAFFIC_CACHE = new Map();
const GLOBAL_DECODER = new TextDecoder();
const GLOBAL_ENCODER = new TextEncoder();
const ACTIVE_CONNECTIONS_COUNT = new Map();
// آرایه جدول هگزادسیمال برای افزایش سرعت استخراج UUID
const LUT_HEX = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, '0'));
// در خطوط ابتدایی برنامه اضافه کنید:
const USERS_LIST_CACHE = { data: null, lastFetch: 0 };
const GLOBAL_LAST_ACTIVE_WRITE = new Map();
const GLOBAL_LAST_DB_WRITE = new Map();
const GLOBAL_WRITE_LOCK = new Map();
const DNS_CACHE = new Map();
const USER_REQ_CACHE = new Map();
let GLOBAL_REQ_COUNT = 0;
let GLOBAL_LAST_REQ_WRITE = 0;
// آدرس موتور پردازشی ریلوِی شما
// آرایه دامنه‌های بک‌آپ ریل‌وی شما (می‌توانید در آینده دامنه‌های بیشتری به این لیست اضافه کنید)
const RAILWAY_BACKENDS = [
    "makevaslim44-production.up.railway.app",
    "seyed84-production.up.railway.app",
    "manvaslam-production-07a1.up.railway.app"
];
const DNS_CACHE_TTL = 5 * 60 * 1000;
const DOH_RESOLVER = "https://1.1.1.1/dns-query";
const UPSTREAM_BUNDLE_TARGET_BYTES = 32 * 1024;
const UPSTREAM_QUEUE_MAX_BYTES = 16 * 1024 * 1024;
const UPSTREAM_QUEUE_MAX_ITEMS = 4096;
const DOWNSTREAM_GRAIN_BYTES = 32 * 1024;
const DOWNSTREAM_GRAIN_TAIL_THRESHOLD = 512;
const DOWNSTREAM_GRAIN_SILENT_MS = 1;
const TCP_CONCURRENCY = 4;
const PRELOAD_RACE_DIAL = true;
export default {
    
	async fetch(request, env, ctx) {
		trackRequest(env, ctx);
		const url = new URL(request.url); // <--- فقط همین یک بار تعریف می‌شود

        // این بلاک را بعد از تعریف متغیر url اضافه کنید:
if (url.pathname === "/api/init-db") {
    try {
        await DbService.ensureSchema(env.DB);
        return new Response("Database initialized successfully.", { 
            status: 200, 
            headers: { "Content-Type": "text/plain; charset=utf-8" } 
        });
    } catch (err) {
        return new Response(`Database init failed: ${err.message}`, { status: 500 });
    }
}
        if (Router.isWebSocketUpgrade(request) && url.pathname.startsWith("/railway-io/")) {
            const extractedUuid = url.pathname.split("/")[2];
            if (extractedUuid) {
                // ✅ اصلاح شد: ctx به عنوان ورودی سوم اضافه شد
                return await handleRailwayWS(request, env, ctx, extractedUuid);
            }
        }
        
        // ✅ حالت استاندارد و بدون ارور
		if (Router.isWebSocketUpgrade(request) && url.pathname === "/Ma_Ke_Vaslim") {
			return await Router.handleWebSocket(request, env, ctx);
		}

		if (Router.isSubscriptionPath(url.pathname)) {
			return await Router.handleSubscription(url, env);
		}
		if (url.pathname.startsWith("/api/") || url.pathname === "/locations") {
			return await Router.handleApi(request, url, env, ctx);
		}
		if (url.pathname === "/panel" || url.pathname === "/login") {
			return await Router.handlePanel(request, env);
		}
		if (url.pathname.startsWith("/status/")) {
    return await Router.handleUserStatus(url, env, request);
}
		return new Response(HTML_TEMPLATES.nginx, {
			headers: { "Content-Type": "text/html; charset=utf-8" },
		});
	},
    async scheduled(_event, env, ctx) {
		ctx.waitUntil(
			(async () => {
				try {
					await env.DB.prepare("UPDATE users SET used_req = 0").run();
					if (typeof USER_REQ_CACHE !== 'undefined' && USER_REQ_CACHE.clear) {
						USER_REQ_CACHE.clear();
					}
					if (typeof USERS_LIST_CACHE !== 'undefined') {
						USERS_LIST_CACHE.data = null;
						USERS_LIST_CACHE.lastFetch = 0;
					}
					console.log("✅ ریکوئست‌های روزانه تمامی کاربران با موفقیت ریست شد.");
				} catch (error) {
					console.error("❌ خطا در ریست روزانه ریکوئست‌ها:", error);
				}
			})()
		);
	}
}; // پایان بلاک export default
const Router = {
	isWebSocketUpgrade(request) {
		const upgradeHeader = (request.headers.get("Upgrade") || "").toLowerCase();
		return upgradeHeader === "websocket";
	},
	isSubscriptionPath(pathname) {
		return pathname.startsWith("/sub/") || pathname.startsWith("/feed/");
	},
	async handleWebSocket(_request, env, ctx) {
		try {
			let proxyIP = "proxyip.cmliussss.net";
			try {
				const proxyRow = await env.DB.prepare("SELECT value FROM settings WHERE key = 'proxy_ip'").first();
				if (proxyRow && proxyRow.value) {
					proxyIP = proxyRow.value;
				}
			} catch (e) {}
			const mockStoredData = { proxy_ip: proxyIP };
			return handleVLESS(env, mockStoredData, ctx, _request);
		} catch (e) {
			return new Response("Internal Server Error", { status: 500 });
		}
	},
	async handleSubscription(url, env) {
		const isSubPath = url.pathname.startsWith("/sub/");
		const offset = isSubPath ? 5 : 6;
		let subUser = decodeURIComponent(url.pathname.slice(offset));
		const host = url.hostname;
		try {
			const user = await env.DB.prepare("SELECT * FROM users WHERE username = ? OR uuid = ?").bind(subUser, subUser).first();
			if (!user || user.connection_type !== atob("dmxlc3M=")) {
				return new Response("Not Found", { status: 404 });
			}
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
        const user = await env.DB.prepare("SELECT * FROM users WHERE username = ? OR uuid = ?").bind(username, username).first();
        if (!user) {
            return new Response("User not found", { status: 404 });
        }

        // ۱. بررسی هدر Accept برای تشخیص مرورگر یا نرم‌افزار
        const acceptHeader = request.headers.get("Accept") || "";
        const isBrowser = acceptHeader.includes("text/html");

        if (isBrowser) {
            // ========== خروجی برای مرورگر (صفحه وضعیت) ==========
            const userData = {
                username: user.username,
                uuid: user.uuid,
                limit_gb: user.limit_gb,
                expiry_days: user.expiry_days,
                used_gb: user.used_gb,
                limit_req: user.limit_req,
                used_req: user.used_req,
                is_active: user.is_active,
                online_count: ACTIVE_CONNECTIONS_COUNT.get(user.username) || 0,
                max_connections: user.max_connections,
                created_at: user.created_at,
                tls: user.tls,
                port: user.port,
                ips: user.ips,
                fingerprint: user.fingerprint || "chrome",
user_socks5: user.user_socks5 || null,
            };
            // ایمن‌سازی JSON برای قرارگیری در HTML
            const safeJson = JSON.stringify(userData).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
            const html = HTML_TEMPLATES.status.replace(
                "/* {{USER_DATA_PLACEHOLDER}} */",
                `window.statusUser = JSON.parse('${safeJson}');`
            );
            return new Response(html, {
                headers: { "Content-Type": "text/html; charset=utf-8" },
            });
        } else {
            // ========== خروجی برای نرم‌افزار (ساب‌اسکریپشن) ==========
            // استفاده از تابع موجود برای تولید کانفیگ‌ها
            return await SubscriptionService.generateText(user, url.hostname);
        }
    } catch (err) {
        return new Response("Error: " + err.message, { status: 500 });
    }
},

	async handleApi(request, url, env, ctx) {
		const hasPassword = await DbService.getPanelPassword(env.DB);
		if (url.pathname === "/api/setup-password" && request.method === "POST") {
			if (hasPassword) {
				return new Response(JSON.stringify({ error: "رمز عبور از قبل تعریف شده است" }), {
					status: 400,
					headers: { "Content-Type": "application/json; charset=utf-8" },
				});
			}
			const { password } = await request.json();
			if (!password || password.length < 4) {
				return new Response(JSON.stringify({ error: "رمز عبور باید حداقل ۴ کاراکتر باشد" }), {
					status: 400,
					headers: { "Content-Type": "application/json; charset=utf-8" },
				});
			}
			const hashed = await DbService.sha256(password);
			await DbService.setPanelPassword(env.DB, hashed);
			return new Response(JSON.stringify({ success: true }), {
				headers: {
					"Content-Type": "application/json; charset=utf-8",
					"Set-Cookie": "panel_session=" + hashed + "; Path=/; HttpOnly; Secure; SameSite=Lax",
				},
			});
		}
		if (url.pathname === "/api/login" && request.method === "POST") {
			const { password } = await request.json();
			const hashedInput = await DbService.sha256(password);
			const storedHash = await DbService.getPanelPassword(env.DB);
			if (storedHash === hashedInput) {
				return new Response(JSON.stringify({ success: true }), {
					headers: {
						"Content-Type": "application/json; charset=utf-8",
						"Set-Cookie": "panel_session=" + storedHash + "; Path=/; HttpOnly; Secure; SameSite=Lax",
					},
				});
			}
			return new Response(JSON.stringify({ error: "رمز عبور اشتباه است" }), {
				status: 401,
				headers: { "Content-Type": "application/json; charset=utf-8" },
			});
		}
		if (url.pathname === "/api/logout" && request.method === "POST") {
			return new Response(JSON.stringify({ success: true }), {
				headers: {
					"Content-Type": "application/json; charset=utf-8",
					"Set-Cookie": "panel_session=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; Secure; SameSite=Lax",
				},
			});
		}
		const authorized = await DbService.verifyApiAuth(request, env);
		if (!authorized) {
			return new Response(JSON.stringify({ error: "Unauthorized" }), {
				status: 401,
				headers: { "Content-Type": "application/json; charset=utf-8" },
			});
		}
		if (url.pathname === "/api/update-panel" && request.method === "POST") {
			if (!env.CF_API_TOKEN || !env.CF_ACCOUNT_ID) {
				return new Response(JSON.stringify({ error: "توکن یا اکانت آیدی کلودفلر تنظیم نشده است. لطفا با سایت زیر اپدیت کنید https://zeus-panel.ir-netlify.workers.dev/" }), { status: 400, headers: { "Content-Type": "application/json" } });
			}
			try {
				const githubRes = await fetch("https://raw.githubusercontent.com/IR-NETLIFY/zeus/refs/heads/main/zeus.js?t=" + Date.now() + Math.random(), {
					headers: {
						"Cache-Control": "no-cache, no-store, must-revalidate",
						Pragma: "no-cache",
						Expires: "0",
					},
				});
				if (!githubRes.ok) throw new Error("خطا در دریافت سورس جدید از گیت‌هاب");
				const newCode = await githubRes.text();
				const scriptName = env.WORKER_NAME || url.hostname.split(".")[0];
				const bindingsRes = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CF_ACCOUNT_ID}/workers/scripts/${scriptName}/bindings`, {
					headers: { Authorization: "Bearer " + env.CF_API_TOKEN },
				});
				const bindingsData = await bindingsRes.json();
				if (!bindingsData.success) throw new Error("عدم دسترسی به تنظیمات ورکر. توکن نامعتبر است.");
				const newBindings = [];
				for (const b of bindingsData.result) {
					if (b.type === "d1") {
						newBindings.push({ type: "d1", name: b.name, id: b.database_id || b.id });
					} else if (b.name === "CF_API_TOKEN") {
						newBindings.push({ type: "secret_text", name: "CF_API_TOKEN", text: env.CF_API_TOKEN });
					} else if (b.name === "CF_ACCOUNT_ID") {
						newBindings.push({ type: "secret_text", name: "CF_ACCOUNT_ID", text: env.CF_ACCOUNT_ID });
					}
				}
				const metadata = {
					main_module: "zeus.js",
					compatibility_date: "2024-02-08",
					bindings: newBindings,
				};
				const formData = new FormData();
				formData.append("metadata", new Blob([JSON.stringify(metadata)], { type: "application/json" }));
				formData.append("zeus.js", new Blob([newCode], { type: "application/javascript+module" }), "zeus.js");
				const deployRes = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CF_ACCOUNT_ID}/workers/scripts/${scriptName}`, {
					method: "PUT",
					headers: { Authorization: "Bearer " + env.CF_API_TOKEN },
					body: formData,
				});
				const deployData = await deployRes.json();
				if (!deployData.success) throw new Error("خطا در اعمال آپدیت در کلودفلر.");
				return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });
			} catch (err) {
				const errorMsg = err.message + " | در صورت عدم موفقیت، از طریق لینک زیر آپدیت کنید: https://zeus-panel.ir-netlify.workers.dev/";
				return new Response(JSON.stringify({ error: errorMsg }), { status: 500, headers: { "Content-Type": "application/json" } });
			}
		}
		if (url.pathname === "/api/change-password" && request.method === "POST") {
			const { current_password, new_password } = await request.json();
			if (!current_password || !new_password) {
				return new Response(JSON.stringify({ error: "رمز عبور فعلی و جدید الزامی هستند" }), {
					status: 400,
					headers: { "Content-Type": "application/json; charset=utf-8" },
				});
			}
			const currentHash = await DbService.sha256(current_password);
			const storedHash = await DbService.getPanelPassword(env.DB);
			if (storedHash && storedHash !== currentHash) {
				return new Response(JSON.stringify({ error: "رمز عبور فعلی اشتباه است" }), {
					status: 401,
					headers: { "Content-Type": "application/json; charset=utf-8" },
				});
			}
			if (new_password.length < 4) {
				return new Response(JSON.stringify({ error: "رمز عبور جدید باید حداقل ۴ کاراکتر باشد" }), {
					status: 400,
					headers: { "Content-Type": "application/json; charset=utf-8" },
				});
			}
			const newHash = await DbService.sha256(new_password);
			await DbService.setPanelPassword(env.DB, newHash);
			return new Response(JSON.stringify({ success: true }), {
				headers: {
					"Content-Type": "application/json; charset=utf-8",
					"Set-Cookie": "panel_session=" + newHash + "; Path=/; HttpOnly; Secure; SameSite=Lax",
				},
			});
		}
		if (url.pathname === "/locations") {
			try {
				const response = await fetch("https://speed.cloudflare.com/locations", {
					headers: { Referer: "https://speed.cloudflare.com/" },
				});
				const data = await response.json();
				return new Response(JSON.stringify(data), {
					headers: { "Content-Type": "application/json; charset=utf-8", "Access-Control-Allow-Origin": "*" },
				});
			} catch (e) {
				return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: { "Content-Type": "application/json" } });
			}
		}
		if (url.pathname === "/api/proxy-ip") {
			if (request.method === "POST") {
				const { proxy_ip, iata, frag_len, frag_int } = await request.json();
				if (proxy_ip) await env.DB.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('proxy_ip', ?)").bind(proxy_ip).run();
				if (iata !== undefined) await env.DB.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('proxy_location_iata', ?)").bind(iata).run();
				if (frag_len !== undefined) await env.DB.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('frag_len', ?)").bind(frag_len).run();
				if (frag_int !== undefined) await env.DB.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('frag_int', ?)").bind(frag_int).run();
				return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });
			}
			if (request.method === "GET") {
				const rowIp = await env.DB.prepare("SELECT value FROM settings WHERE key = 'proxy_ip'").first();
				const rowIata = await env.DB.prepare("SELECT value FROM settings WHERE key = 'proxy_location_iata'").first();
				const rowLen = await env.DB.prepare("SELECT value FROM settings WHERE key = 'frag_len'").first();
				const rowInt = await env.DB.prepare("SELECT value FROM settings WHERE key = 'frag_int'").first();
				return new Response(
					JSON.stringify({
						proxy_ip: rowIp ? rowIp.value : "proxyip.cmliussss.net",
						iata: rowIata ? rowIata.value : "",
						frag_len: rowLen ? rowLen.value : "20-30",
						frag_int: rowInt ? rowInt.value : "1-2",
					}),
					{ headers: { "Content-Type": "application/json" } }
				);
			}
		}
		if (url.pathname.startsWith("/api/users")) {
			const pathParts = url.pathname.split("/");
			const isUserAction = pathParts.length > 3;
			if (isUserAction) {
				const username = decodeURIComponent(pathParts.pop());
				if (request.method === "PUT") {
					const body = await request.json();
					if (body.toggle_only !== undefined) {
						await env.DB.prepare("UPDATE users SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END WHERE username = ?").bind(username).run();
						// بعد از تغییر کاربر، کش لیست کاربران را منقضی می‌کنیم تا اطلاعات سریع بروز شوند
						USERS_LIST_CACHE.data = null;
USERS_LIST_CACHE.lastFetch = 0;
						return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });
					} else if (body.reset_action !== undefined) {
						if (body.reset_action === "volume") {
							await env.DB.prepare("UPDATE users SET used_gb = 0 WHERE username = ?").bind(username).run();
							GLOBAL_TRAFFIC_CACHE.set(username, 0);
						} else if (body.reset_action === "req") {
							await env.DB.prepare("UPDATE users SET used_req = 0 WHERE username = ?").bind(username).run();
							USER_REQ_CACHE.set(username, 0);
						} else if (body.reset_action === "time") {
							await env.DB.prepare("UPDATE users SET created_at = CURRENT_TIMESTAMP WHERE username = ?").bind(username).run();
						}
						USERS_LIST_CACHE.data = null;
USERS_LIST_CACHE.lastFetch = 0;
						return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });
					} else {
						const { username: new_username, limit_gb, expiry_days, limit_req, ips, tls, port, fingerprint, max_connections, user_socks5 } = body;
						if (new_username && new_username !== username) {
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
						await env.DB.prepare(`UPDATE users SET username = ?, limit_gb = ?, expiry_days = ?, limit_req = ?, ips = ?, tls = ?, port = ?, fingerprint = ?, max_connections = ?, user_socks5 = ? WHERE username = ?`)
    .bind(new_username || username, limit_gb ? parseFloat(limit_gb) : null, expiry_days ? parseInt(expiry_days) : null, limit_req ? parseInt(limit_req) : null, ips || null, tls, port, fingerprint || "chrome", max_connections ? parseInt(max_connections) : null, user_socks5 || null, username)
    .run();
						USERS_LIST_CACHE.data = null;
USERS_LIST_CACHE.lastFetch = 0;
						return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });
					}
				}
				if (request.method === "DELETE") {
					await env.DB.prepare("DELETE FROM users WHERE username = ?").bind(username).run();
					USERS_LIST_CACHE.data = null;
USERS_LIST_CACHE.lastFetch = 0;
					return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });
				}
			} else {
				if (request.method === "GET") {
					const now = Date.now();
					// اعمال درست سیستم کش ۳۰ ثانیه‌ای در مسیر لود کاربران پنل زئوس
					if (USERS_LIST_CACHE.data && (now - USERS_LIST_CACHE.lastFetch < 30000)) {
						return new Response(USERS_LIST_CACHE.data, {
							headers: {
								"Content-Type": "application/json; charset=utf-8",
								"Cache-Control": "max-age=30"
							},
						});
					}

					try {
						await flushExpiredTraffic(env);
					} catch (e) {}
					const { results } = await env.DB.prepare("SELECT * FROM users ORDER BY id DESC").all();
					const enrichedUsers = (results || []).map((user) => ({
						...user,
						is_online: user.last_active && now - user.last_active < 65000 ? 1 : 0,
						online_count: ACTIVE_CONNECTIONS_COUNT.get(user.username) || 0,
					}));
					let cfReqs = { today: 0, total: 0 };
					try {
						const liveCf = await getCfUsage(env);
						const todayStr = new Date().toISOString().split("T")[0];
						const dateRow = await env.DB.prepare("SELECT value FROM settings WHERE key = 'req_last_date'").first();
						const totalRow = await env.DB.prepare("SELECT value FROM settings WHERE key = 'req_total'").first();
						let dbTotal = totalRow ? parseInt(totalRow.value) || 0 : 0;
						let dbToday = 0;
						if (dateRow && dateRow.value === todayStr) {
							const todayRow = await env.DB.prepare("SELECT value FROM settings WHERE key = 'req_today'").first();
							dbToday = todayRow ? parseInt(todayRow.value) || 0 : 0;
						}
						if (liveCf.today > dbToday) {
							dbToday = liveCf.today;
							await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_today', ?) ON CONFLICT(key) DO UPDATE SET value = ?").bind(String(dbToday), String(dbToday)).run();
							await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_last_date', ?) ON CONFLICT(key) DO UPDATE SET value = ?").bind(todayStr, todayStr).run();
						}
						if (liveCf.total > dbTotal) {
							dbTotal = liveCf.total;
							await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_total', ?) ON CONFLICT(key) DO UPDATE SET value = ?").bind(String(dbTotal), String(dbTotal)).run();
						}
						cfReqs.today = dbToday + GLOBAL_REQ_COUNT;
						cfReqs.total = dbTotal + GLOBAL_REQ_COUNT;
					} catch (e) {}

					const finalResponseData = JSON.stringify({
						users: enrichedUsers,
						serverTime: now,
						cfRequestsToday: cfReqs.today,
						cfRequestsTotal: cfReqs.total,
					});

					// ذخیره در کش
					USERS_LIST_CACHE.data = finalResponseData;
					USERS_LIST_CACHE.lastFetch = now;

					return new Response(finalResponseData, {
						headers: {
							"Content-Type": "application/json; charset=utf-8",
							"Cache-Control": "max-age=30",
						},
					});
				}
				const { username, uuid, limit_gb, expiry_days, limit_req, ips, tls, port, fingerprint, max_connections, used_gb, used_req, created_at, is_active, user_socks5 } = await request.json();
					if (!username) {
						return new Response(JSON.stringify({ error: "نام کاربری اجباری است" }), { status: 400, headers: { "Content-Type": "application/json" } });
					}
					const finalUuid = uuid || crypto.randomUUID();
					const parsedUsedGb = parseFloat(used_gb);
					const finalUsedGb = !isNaN(parsedUsedGb) ? parsedUsedGb : 0;
					const parsedUsedReq = parseInt(used_req);
					const finalUsedReq = !isNaN(parsedUsedReq) ? parsedUsedReq : 0;
					const finalCreatedAt = created_at || new Date().toISOString();
					const parsedIsActive = parseInt(is_active);
					const finalIsActive = !isNaN(parsedIsActive) ? parsedIsActive : 1;
					try {
						await env.DB.prepare(`INSERT INTO users (username, uuid, limit_gb, expiry_days, limit_req, ips, connection_type, tls, port, fingerprint, max_connections, used_gb, used_req, created_at, is_active, user_socks5) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .bind(username, finalUuid, limit_gb ? parseFloat(limit_gb) : null, expiry_days ? parseInt(expiry_days) : null, limit_req ? parseInt(limit_req) : null, ips || null, atob("dmxlc3M="), tls, port, fingerprint || "chrome", max_connections ? parseInt(max_connections) : null, finalUsedGb, finalUsedReq, finalCreatedAt, finalIsActive, user_socks5 || null)
    .run();
						USERS_LIST_CACHE.data = null;
USERS_LIST_CACHE.lastFetch = 0;
						return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });
					} catch (err) {
						return new Response(JSON.stringify({ error: err.message }), { status: 500, headers: { "Content-Type": "application/json" } });
					}
				}
			}
		}
if (url.pathname === "/api/test-proxy" && request.method === "POST") {
    const { proxy } = await request.json();
    if (!proxy) return new Response(JSON.stringify({ error: "پـروکـسـی وارد نشده است" }), { status: 400, headers: { "Content-Type": "application/json" } });
    try {
        let ip = "";
        let workingProxy = proxy;
        if (proxy.includes("t.me/socks") || proxy.includes("tg://socks")) {
            ip = proxy.match(/server=([^&]+)/)?.[1] || "";
        } else {
            let cleanProxy = proxy.replace(/^(socks4|socks5|socks|http|https):\/\//i, "");
            let remain = cleanProxy;
            if (remain.includes("@")) remain = remain.substring(remain.lastIndexOf("@") + 1);
            if (remain.startsWith("[")) {
                ip = remain.substring(1, remain.indexOf("]"));
            } else {
                const lastColon = remain.lastIndexOf(":");
                if (lastColon !== -1 && remain.indexOf(":") === lastColon) ip = remain.substring(0, lastColon);
                else ip = remain;
            }
        }
        let country = "UN";
        if (ip) {
            try {
                const geoRes = await fetch(`http://ip-api.com/json/${ip}?fields=countryCode`);
                const geoData = await geoRes.json();
                if (geoData && geoData.countryCode) country = geoData.countryCode;
            } catch (e) {}
        }
        const startTime = Date.now();
        const payload = new TextEncoder().encode("GET / HTTP/1.1\r\nHost: 1.1.1.1\r\nConnection: close\r\n\r\n");
        const s = await connectProxy(proxy, "1.1.1.1", 80, payload);
        const reader = s.readable.getReader();
        const res = await reader.read();
        if (res.done || !res.value) {
            s.close();
            throw new Error("تایم‌اوت در دریافت دیتا");
        }
        s.close();
        const ping = Date.now() - startTime;
        return new Response(JSON.stringify({ success: true, ping, country }), { headers: { "Content-Type": "application/json" } });
    } catch (e) {
        let msg = e.message;
        if (msg.includes("Stream was cancelled") || msg.includes("network")) msg = "ارتباط با سرور قطع شد (احتمالاً پروکسی مسدود یا خاموش است)";
        else if (msg.includes("timeout") || msg.includes("timed out") || msg.includes("تایم‌اوت")) msg = "تایم‌اوت در اتصال (پروکسی در دسترس نیست)";
        else if (msg.includes("Invalid URL") || msg.includes("Invalid format")) msg = "فرمت وارد شده برای پروکسی اشتباه است";
        else msg = "خطای نامشخص";
        return new Response(JSON.stringify({ error: msg }), { status: 500, headers: { "Content-Type": "application/json" } });
    }
}
		return new Response(JSON.stringify({ error: "Not Found" }), { status: 404, headers: { "Content-Type": "application/json" } });
	},
};
let schemaEnsured = false;
let cachedPanelPassword = null;
const DbService = {
	async ensureSchema(db) {
		if (schemaEnsured) return;
		try {
			await db
				.prepare(
					`
        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          username TEXT UNIQUE,
          uuid TEXT,
          limit_gb REAL,
          expiry_days INTEGER,
          ips TEXT,
          connection_type TEXT,
          tls TEXT,
          port INTEGER,
          used_gb REAL DEFAULT 0,
          is_active INTEGER DEFAULT 1,
          last_active INTEGER,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `,
				)
				.run();
		} catch (e) {}
try {
    await db.prepare("ALTER TABLE users ADD COLUMN user_socks5 TEXT DEFAULT NULL").run();
} catch (e) {}
		try {
			await db.prepare("ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1").run();
		} catch (e) {}
		try {
			await db.prepare("ALTER TABLE users ADD COLUMN last_active INTEGER").run();
		} catch (e) {}
		try {
			await db.prepare("ALTER TABLE users ADD COLUMN fingerprint TEXT DEFAULT 'chrome'").run();
		} catch (e) {}
		try {
			await db.prepare("ALTER TABLE users ADD COLUMN max_connections INTEGER").run();
		} catch (e) {}
		try {
			await db.prepare("ALTER TABLE users ADD COLUMN limit_req INTEGER").run();
		} catch (e) {}
		try {
			await db.prepare("ALTER TABLE users ADD COLUMN used_req INTEGER DEFAULT 0").run();
		} catch (e) {}
		try {
			await db.prepare("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)").run();
		} catch (e) {}
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
		const sessionCookie = cookies.split(";").find((c) => c.trim().startsWith("panel_session="));
		if (!sessionCookie) return false;
		const sessionToken = sessionCookie.split("=")[1].trim();
		return sessionToken === storedPasswordHash;
	},
	async sha256(message) {
		const msgBuffer = new TextEncoder().encode(message);
		const hashBuffer = await crypto.subtle.digest("SHA-256", msgBuffer);
		const hashArray = Array.from(new Uint8Array(hashBuffer));
		return hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
	},
};
const SubscriptionService = {
    async generateText(user, host) {
        // ۱. دامین فیلتر شده (اگر واقعاً می‌خواهید SNI متفاوت باشد، در بخش params در پایین از آن استفاده کنید)
        const mySecretDomain = "mkvaslim.mkvasli.workers.dev"; 

        const rawIpList = [
            "add555.musicalls.ir:443#A🇹🇷",
            "ip3.synthara.ir:443#B🇹🇷",
            "cf-mtn.coixconqwe1.ir:443#C🇫🇮",
            "104.17.122.116:2053#D🇫🇮",
            "support.zoom.us:443#E🇫🇷",
            "104.16.174.118:443#F🇫🇷",
            "cf-mci.coixconqwe1.ir:443#G🇩🇪",
            "69.84.182.49:443#H🇩🇪",
            "88.198.82.158:443#I🇩🇪",
            "104.17.121.228:443#J🇩🇪",
            "104.16.71.14:443#K🇩🇪",
            "104.17.122.116:2096#L🇩🇪",
            "104.17.107.119:443#M🇳🇱",
            "104.16.72.251:443#N🇳🇱",
            "158.180.231.216:443#O🇸🇪",
            "record20.zeus744.ir:443#P🇸🇪",
            "Abot.sahoria.ir:2053#Q🇺🇸",
            "104.18.154.96:443#R🇺🇸",
            "198.244.235.173:179#S🇬🇧",
            "57.128.176.37:443#T🇬🇧",
            "104.17.121.229:443#U🇬🇧",
            "104.18.1.1:443#V✅",
            "discourse.ir:443#W⚡",
            "104.18.153.252:443#X🔥",
            "104.17.122.116:2083#آلمان1🇩🇪",
            "104.17.122.116:2053#2آلمان‌🇩🇪",
            "141.101.114.183:443#ایرانسل1💛",
            "104.16.241.142:2083#ایرانسل1💛🇦🇿",
            "173.245.58.187:2096#ایرانسل2💛🇦🇿",
            "104.25.73.59:2083#ایرانسل2💛",
            "104.21.3.125:443#ایرانسل💛📡",
            "162.159.248.12:443#ایرانسل💛🚀",
            "172.67.223.97:443#ایرانسل💛⚔️",
            "172.67.113.199:443#ایرانسل💛⚡",
            "167.71.45.93:443#ایرانسل💛✅",
            "178.250.187.110:443#ایرانسل💛🛰️",
            "104.16.1.1:443#ایرانسل💛🔥",
            "cf-mci.coixconqwe1.ir:443#Gaming🎮"
        ];

        // محاسبات حجم و زمان باقی‌مانده
        let remVol = "∞";
        if (user.limit_gb) {
            let rem = user.limit_gb - (user.used_gb || 0);
            remVol = rem > 0 ? rem.toFixed(1) + "GB" : "0GB";
        }
        
        let remTime = "∞";
        if (user.expiry_days && user.created_at) {
            const created = new Date(user.created_at);
            const expiryDate = new Date(created.getTime() + user.expiry_days * 24 * 60 * 60 * 1000);
            const diffDays = Math.ceil((expiryDate.getTime() - Date.now()) / (1000 * 60 * 60 * 24));
            remTime = diffDays > 0 ? diffDays + "D" : "0D";
        }
        
        const info = `[${remVol}|${remTime}]`;
        const fp = user.fingerprint || "chrome"; // گرفتن فینگرپرینت واقعی کاربر
        const links = [];

        // انتقال تابع به بیرون از حلقه برای پرفورمنس بالاتر
        function generateRailwayConfig(userUuid, workerHost, cleanIp, port, configName, fingerprint, fragStr, extraStr) {
            const encodedName = encodeURIComponent(configName);
            const masterUuid = "e5b8a6a1-a7b3-4f16-89d2-97b7914db459"; // حتماً این مقدار را به ENV منتقل کنید!
            
            const params = new URLSearchParams({
                encryption: "none",
                security: "tls",
                sni: mySecretDomain, // در صورت نیاز می‌توانید workerHost را با mySecretDomain جایگزین کنید
                host: mySecretDomain, 
                fp: fingerprint, // اعمال Fingerprint واقعی
                type: "ws", 
                path: `/railway-io/${userUuid}` ,
                alpn: "h2,http/1.1"
            });
            
            // تزریق متغیرهای هوشمند به انتهای لینک کانفیگ
            return `vless://${masterUuid}@${cleanIp}:${port}?${params.toString()}${fragStr}&extraParams=${extraStr}#${encodedName}`;
        }

        // حلقه اصلی ساخت کانفیگ‌ها
        rawIpList.forEach((item) => {
            const [addressPart, customName] = item.split('#');
            const [ipOrDomain, port] = addressPart.split(':');

            if (ipOrDomain && port && customName) {
                // اعمال متغیر remark
                const remark = `${customName}--->🇳🇱 ${info}`;
                const lowerName = customName.toLowerCase();
                
                let currentFragment = "&fragment=100-200,10-20";
                let extraParamsObj = {
                    "mode": "auto",
                    "xPaddingBytes": "100-1000",
                    "xmux": { "maxConcurrency": "4-8" }
                };

                // منطق مسیریابی پویا
                if (lowerName.includes("mci") || lowerName.includes("همراه")) {
                    currentFragment = "&fragment=10-20,10-20";
                    extraParamsObj.xPaddingBytes = "100-500";
                    extraParamsObj.xmux.maxConcurrency = "4-8";
                } 
                else if (lowerName.includes("irancell") || lowerName.includes("ایرانسل")) {
                    currentFragment = "&fragment=100-200,10-20";
                    extraParamsObj.xPaddingBytes = "500-1500";
                    extraParamsObj.xmux.maxConcurrency = "4-8";
                } 
                else if (lowerName.includes("gaming") || lowerName.includes("گیم")) {
                    currentFragment = ""; 
                    extraParamsObj.xmux.maxConcurrency = "1-4";
                }

                const extraParams = encodeURIComponent(JSON.stringify(extraParamsObj));

                // پاس دادن تمام متغیرها به تابع سازنده
                const linkRailway = generateRailwayConfig(user.uuid, host, ipOrDomain, port, remark, fp, currentFragment, extraParams);
                links.push(linkRailway);
            }
        });

        // خروجی نهایی به صورت Base64
        const subContent = btoa(unescape(encodeURIComponent(links.join("\n"))));
        return new Response(subContent, {
            headers: {
                "Content-Type": "text/plain; charset=utf-8",
                "Subscription-Userinfo": `upload=0; download=${Math.floor((user.used_gb || 0) * 1073741824)}; total=${user.limit_gb ? Math.floor(user.limit_gb * 1073741824) : 0}`
            },
        });
    },
};
            
async function flushExpiredTraffic(env) {
	const now = Date.now();
	for (const [uname, cachedBytes] of GLOBAL_TRAFFIC_CACHE.entries()) {
		const cachedReqs = USER_REQ_CACHE.get(uname) || 0;
		if (cachedBytes <= 0 && cachedReqs <= 0) continue;
		if (GLOBAL_WRITE_LOCK.get(uname)) continue;
		const lastActive = GLOBAL_LAST_ACTIVE_WRITE.get(uname) || 0;
		const activeCount = ACTIVE_CONNECTIONS_COUNT.get(uname) || 0;
		if (activeCount <= 0 || now - lastActive > 65000) {
			GLOBAL_WRITE_LOCK.set(uname, true);
			const deltaGb = cachedBytes / (1024 * 1024 * 1024);
			try {
				await env.DB.prepare("UPDATE users SET used_gb = used_gb + ?, used_req = used_req + ? WHERE username = ?").bind(deltaGb, cachedReqs, uname).run();
			} catch (e) {
				console.error(e.message);
			} finally {
				GLOBAL_WRITE_LOCK.delete(uname);
				GLOBAL_TRAFFIC_CACHE.delete(uname);
				USER_REQ_CACHE.delete(uname);
				GLOBAL_LAST_ACTIVE_WRITE.delete(uname);
			}
		}
	}
}
async function connectProxy(proxyStr, destAddr, destPort, initialData) {
	let normalized = proxyStr;
	if (proxyStr.includes("t.me/socks") || proxyStr.includes("tg://socks")) {
		const server = proxyStr.match(/server=([^&]+)/)?.[1];
		const port = proxyStr.match(/port=([^&]+)/)?.[1];
		const user = proxyStr.match(/user=([^&]+)/)?.[1];
		const pass = proxyStr.match(/pass=([^&]+)/)?.[1];
		if (server && port) {
			normalized = user && pass ? `socks5://${user}:${pass}@${server}:${port}` : `socks5://${server}:${port}`;
		}
	}
	const isHttp = normalized.toLowerCase().startsWith("http://") || normalized.toLowerCase().startsWith("https://");
	const isSocks4 = normalized.toLowerCase().startsWith("socks4://");
	let cleanStr = normalized.replace(/^(socks4|socks5|socks|http|https):\/\//i, "");
	if (isHttp) {
		return await connectHttp(cleanStr, destAddr, destPort, initialData);
	}
	if (isSocks4) {
		return await connectSocks4(cleanStr, destAddr, destPort, initialData);
	}
	return await connectSocks5(cleanStr, destAddr, destPort, initialData);
}
async function connectSocks4(proxyStr, destAddr, destPort, initialData) {
	const { user, pass, host, port, auth } = parseProxyConfig(proxyStr, 1080);
	const socket = connect({ hostname: host, port: port });
	const reader = socket.readable.getReader();
	const writer = socket.writable.getWriter();
	try {
		const portHigh = (destPort >> 8) & 0xff;
		const portLow = destPort & 0xff;
		let req;
		if (isIPv4(destAddr)) {
			const ipBytes = destAddr.split(".").map(Number);
			req = new Uint8Array([0x04, 0x01, portHigh, portLow, ipBytes[0], ipBytes[1], ipBytes[2], ipBytes[3], 0x00]);
		} else {
			const hostBytes = new TextEncoder().encode(destAddr);
			req = new Uint8Array(9 + hostBytes.length + 1);
			req[0] = 0x04;
			req[1] = 0x01;
			req[2] = portHigh;
			req[3] = portLow;
			req[4] = 0x00;
			req[5] = 0x00;
			req[6] = 0x00;
			req[7] = 0x01;
			req[8] = 0x00;
			req.set(hostBytes, 9);
			req[9 + hostBytes.length] = 0x00;
		}
		await writer.write(req);
		let res = await reader.read();
		if (res.done || !res.value || res.value[0] !== 0x00 || res.value[1] !== 0x5a) {
			throw new Error("پـروکـسـی SOCKS4 وصل نشد یا اتصال را رد کرد");
		}
		if (initialData && initialData.byteLength > 0) {
			await writer.write(convertToUint8Array(initialData));
		}
		writer.releaseLock();
		reader.releaseLock();
		return socket;
	} catch (e) {
		try {
			writer.releaseLock();
		} catch (err) {}
		try {
			reader.releaseLock();
		} catch (err) {}
		try {
			socket.close();
		} catch (err) {}
		throw e;
	}
}
function parseProxyConfig(proxyStr, defaultPort) {
	let user = "",
		pass = "",
		host = "",
		port = defaultPort;
	let auth = false,
		remain = proxyStr;
	if (remain.includes("@")) {
		const atIdx = remain.lastIndexOf("@");
		const authPart = remain.substring(0, atIdx);
		remain = remain.substring(atIdx + 1);
		const colonIdx = authPart.indexOf(":");
		if (colonIdx !== -1) {
			user = authPart.substring(0, colonIdx);
			pass = authPart.substring(colonIdx + 1);
		} else {
			user = authPart;
		}
		auth = true;
	}
	if (remain.startsWith("[")) {
		const closeIdx = remain.indexOf("]");
		if (closeIdx !== -1) {
			host = remain.substring(1, closeIdx);
			if (remain.length > closeIdx + 1 && remain[closeIdx + 1] === ":") port = parseInt(remain.substring(closeIdx + 2)) || defaultPort;
		}
	} else {
		const lastColon = remain.lastIndexOf(":");
		if (lastColon !== -1 && remain.indexOf(":") === lastColon) {
			host = remain.substring(0, lastColon);
			port = parseInt(remain.substring(lastColon + 1)) || defaultPort;
		} else {
			host = remain;
		}
	}
	return { user, pass, host, port, auth };
}
async function connectSocks5(socksStr, destAddr, destPort, initialData) {
	const { user, pass, host, port, auth } = parseProxyConfig(socksStr, 1080);
	const socket = connect({ hostname: host, port: port });
	const reader = socket.readable.getReader();
	const writer = socket.writable.getWriter();
	try {
		if (auth) {
			await writer.write(new Uint8Array([0x05, 0x02, 0x00, 0x02]));
		} else {
			await writer.write(new Uint8Array([0x05, 0x01, 0x00]));
		}
		let res = await reader.read();
		if (res.done || !res.value || res.value[0] !== 0x05) throw new Error("پاسخ نامعتبر از سرور (پـروکـسـی SOCKS5 نیست یا خاموش است)");
		const method = res.value[1];
		if (method === 0x02) {
			const uEnc = new TextEncoder().encode(user);
			const pEnc = new TextEncoder().encode(pass);
			const authReq = new Uint8Array(1 + 1 + uEnc.length + 1 + pEnc.length);
			authReq[0] = 0x01;
			authReq[1] = uEnc.length;
			authReq.set(uEnc, 2);
			authReq[2 + uEnc.length] = pEnc.length;
			authReq.set(pEnc, 3 + uEnc.length);
			await writer.write(authReq);
			let authRes = await reader.read();
			if (authRes.done || !authRes.value || authRes.value[1] !== 0x00) throw new Error("نام کاربری یا رمز عبور پـروکـسـی اشتباه است");
		}
		let addrType = 0x03;
		let addrBytes;
		if (isIPv4(destAddr)) {
			addrType = 0x01;
			addrBytes = new Uint8Array(destAddr.split(".").map(Number));
		} else if (destAddr.includes(":")) {
			addrType = 0x04;
			addrBytes = new Uint8Array(16);
			const blocks = destAddr.split(":");
			for (let i = 0; i < 8; i++) {
				const val = parseInt(blocks[i] || "0", 16);
				addrBytes[i * 2] = (val >> 8) & 0xff;
				addrBytes[i * 2 + 1] = val & 0xff;
			}
		} else {
			const enc = new TextEncoder().encode(destAddr);
			addrBytes = new Uint8Array(1 + enc.length);
			addrBytes[0] = enc.length;
			addrBytes.set(enc, 1);
		}
		const req = new Uint8Array(4 + addrBytes.length + 2);
		req[0] = 0x05;
		req[1] = 0x01;
		req[2] = 0x00;
		req[3] = addrType;
		req.set(addrBytes, 4);
		const portOffset = 4 + addrBytes.length;
		req[portOffset] = (destPort >> 8) & 0xff;
		req[portOffset + 1] = destPort & 0xff;
		await writer.write(req);
		let connRes = await reader.read();
		if (connRes.done || !connRes.value || connRes.value[1] !== 0x00) throw new Error("پـروکـسـی وصل شد اما دسترسی به اینترنت آزاد ندارد");
		if (initialData && initialData.byteLength > 0) {
			await writer.write(convertToUint8Array(initialData));
		}
		writer.releaseLock();
		reader.releaseLock();
		return socket;
	} catch (e) {
		try {
			writer.releaseLock();
		} catch (err) {}
		try {
			reader.releaseLock();
		} catch (err) {}
		try {
			socket.close();
		} catch (err) {}
		throw e;
	}
}
async function connectHttp(proxyStr, destAddr, destPort, initialData) {
	const { user, pass, host, port, auth } = parseProxyConfig(proxyStr, 80);
	const socket = connect({ hostname: host, port: port });
	const reader = socket.readable.getReader();
	const writer = socket.writable.getWriter();
	try {
		const safeDest = destAddr.includes(":") ? `[${destAddr}]` : destAddr;
		let req = `CONNECT ${safeDest}:${destPort} HTTP/1.1\r\nHost: ${safeDest}:${destPort}\r\n`;
		if (auth) {
			const authBase64 = btoa(`${user}:${pass}`);
			req += `Proxy-Authorization: Basic ${authBase64}\r\n`;
		}
		req += "\r\n";
		await writer.write(new TextEncoder().encode(req));
		let resStr = "";
		while (true) {
			const res = await reader.read();
			if (res.done || !res.value) throw new Error("proxy_closed");
			resStr += new TextDecoder().decode(res.value, { stream: true });
			if (resStr.includes("\r\n\r\n")) {
				const match = resStr.match(/^HTTP\/\d\.\d\s+(\d+)/);
				if (match && match[1] === "200") {
					break;
				} else {
					throw new Error("proxy_error_" + (match ? match[1] : "unknown"));
				}
			}
		}
		if (initialData && initialData.byteLength > 0) {
			await writer.write(convertToUint8Array(initialData));
		}
		writer.releaseLock();
		reader.releaseLock();
		return socket;
	} catch (e) {
		try {
			writer.releaseLock();
		} catch (err) {}
		try {
			reader.releaseLock();
		} catch (err) {}
		try {
			socket.close();
		} catch (err) {}
		throw e;
	}
}
async function handleVLESS(env, storedData = null, ctx = null, request = null) {
	const socketPair = new WebSocketPair();
	const [clientSock, serverSock] = Object.values(socketPair);
	serverSock.accept();
	serverSock.binaryType = "arraybuffer";
	let username = null;
	let tickCount = 0;
	let validUUID = null;
	function addBytes(bytes) {
		if (bytes <= 0) return;
		if (!username) {
			uncountedBytes += bytes;
			return;
		}
		if (uncountedBytes > 0) {
			bytes += uncountedBytes;
			uncountedBytes = 0;
		}
		let current = GLOBAL_TRAFFIC_CACHE.get(username) || 0;
		GLOBAL_TRAFFIC_CACHE.set(username, current + bytes);
		GLOBAL_LAST_ACTIVE_WRITE.set(username, Date.now());
		if (GLOBAL_WRITE_LOCK.get(username)) return;
		let lastDbWrite = GLOBAL_LAST_DB_WRITE.get(username) || 0;
		let now = Date.now();
		let thresholdBytes = 10 * 1024 * 1024;
		if (current >= thresholdBytes || (current > 0 && now - lastDbWrite > 60000)) {
			GLOBAL_WRITE_LOCK.set(username, true);
			let toCommit = GLOBAL_TRAFFIC_CACHE.get(username) || 0;
			let toCommitReq = USER_REQ_CACHE.get(username) || 0;
			if (toCommit <= 0 && toCommitReq <= 0) {
				GLOBAL_WRITE_LOCK.set(username, false);
				return;
			}
			GLOBAL_TRAFFIC_CACHE.set(username, 0);
			USER_REQ_CACHE.set(username, 0);
			GLOBAL_LAST_DB_WRITE.set(username, now);
			let deltaGb = toCommit / (1024 * 1024 * 1024);
			let writeTask = async () => {
				try {
					await env.DB.prepare("UPDATE users SET used_gb = used_gb + ?, used_req = used_req + ? WHERE username = ?").bind(deltaGb, toCommitReq, username).run();
				} catch (e) {
					console.error(e.message);
				} finally {
					GLOBAL_WRITE_LOCK.set(username, false);
				}
			};
			if (ctx) ctx.waitUntil(writeTask());
			else writeTask();
		}
	}
	let isOfflineSet = false;
	const setOffline = () => {
		if (isOfflineSet) return;
		isOfflineSet = true;
		const uname = username;
		if (!uname) return;
		let activeCount = ACTIVE_CONNECTIONS_COUNT.get(uname) || 1;
		activeCount = activeCount - 1;
		if (activeCount <= 0) {
			ACTIVE_CONNECTIONS_COUNT.delete(uname);
			let cachedBytes = GLOBAL_TRAFFIC_CACHE.get(uname) || 0;
			let cachedReqs = USER_REQ_CACHE.get(uname) || 0;
			if ((cachedBytes > 0 || cachedReqs > 0) && !GLOBAL_WRITE_LOCK.get(uname)) {
				GLOBAL_WRITE_LOCK.set(uname, true);
				const deltaGb = cachedBytes / (1024 * 1024 * 1024);
				const writeTask = async () => {
					try {
						await env.DB.prepare("UPDATE users SET used_gb = used_gb + ?, used_req = used_req + ? WHERE username = ?").bind(deltaGb, cachedReqs, uname).run();
					} catch (e) {
						console.error(e.message);
					} finally {
						GLOBAL_WRITE_LOCK.delete(uname);
						GLOBAL_TRAFFIC_CACHE.delete(uname);
						USER_REQ_CACHE.delete(uname);
						GLOBAL_LAST_ACTIVE_WRITE.delete(uname);
					}
				};
				if (ctx) {
					ctx.waitUntil(writeTask());
				} else {
					writeTask();
				}
			} else {
				GLOBAL_TRAFFIC_CACHE.delete(uname);
				USER_REQ_CACHE.delete(uname);
				GLOBAL_LAST_ACTIVE_WRITE.delete(uname);
				GLOBAL_WRITE_LOCK.delete(uname);
			}
		} else {
			ACTIVE_CONNECTIONS_COUNT.set(uname, activeCount);
		}
	};
	const heartbeat = setInterval(async () => {
		if (serverSock.readyState === WebSocket.OPEN) {
			try {
				serverSock.send(new Uint8Array(0));
				if (!validUUID) return;
				tickCount++;
				if (tickCount >= 4) {
					tickCount = 0;
					const user = await env.DB.prepare("SELECT is_active, limit_gb, used_gb, limit_req, used_req, expiry_days, created_at FROM users WHERE uuid = ?").bind(validUUID).first();
					let isExpired = false;
					if (!user || user.is_active === 0) {
						isExpired = true;
					} else {
						if (user.limit_gb && user.used_gb >= user.limit_gb) {
							isExpired = true;
						}
						if (user.limit_req && user.used_req + (USER_REQ_CACHE.get(username) || 0) >= user.limit_req) {
							isExpired = true;
						}
						if (user.expiry_days && user.created_at) {
							const created = new Date(user.created_at);
							const expiryDate = new Date(created.getTime() + user.expiry_days * 24 * 60 * 60 * 1000);
							if (new Date() > expiryDate) {
								isExpired = true;
							}
						}
					}
					if (isExpired) {
						await env.DB.prepare("UPDATE users SET is_active = 0, last_active = 0 WHERE uuid = ?").bind(validUUID).run();
						clearInterval(heartbeat);
						closeSocketQuietly(serverSock);
						return;
					}
					const now = Date.now();
					const lastRecorded = GLOBAL_LAST_ACTIVE_WRITE.get(username) || 0;
					if (now - lastRecorded > 15000) {
						GLOBAL_LAST_ACTIVE_WRITE.set(username, now);
						await env.DB.prepare("UPDATE users SET last_active = ? WHERE username = ?").bind(now, username).run();
					}
				}
			} catch (e) {}
		} else {
			clearInterval(heartbeat);
		}
	}, 15000);
	let remoteConnWrapper = { socket: null, connectingPromise: null, retryConnect: null };
	let reqUUID = null;
	let isHeaderParsed = false;
	let isHeaderParsing = false;
	let isDnsQuery = false;
	let chunkBuffer = new Uint8Array(0);
	let uncountedBytes = 0;
	const proxyIP = storedData?.proxy_ip || "proxyip.cmliussss.net";
	let wsChain = Promise.resolve();
	let wsStopped = false,
		wsFailed = false,
		wsFinished = false;
	let wsQueueBytes = 0,
		wsQueueItems = 0;
	let currentSocketWriter = null,
		activeRemoteWriter = null;
	const releaseRemoteWriter = () => {
		if (activeRemoteWriter) {
			try {
				activeRemoteWriter.releaseLock();
			} catch (e) {}
			activeRemoteWriter = null;
		}
		currentSocketWriter = null;
	};
	const getRemoteWriter = () => {
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
		retryConnect: async () => {
			if (typeof remoteConnWrapper.retryConnect === "function") {
				await remoteConnWrapper.retryConnect();
			}
		},
		closeConnection: () => {
			try {
				remoteConnWrapper.socket?.close();
			} catch (e) {}
			closeSocketQuietly(serverSock);
		},
		name: "VlessWSQueue",
	});
	const writeToRemote = async (chunk, allowRetry = true) => {
		return upstreamQueue.writeAndAwait(chunk, allowRetry);
	};
	const processWsMessage = async (chunk) => {
		const bytes = chunk.byteLength || 0;
		await addBytes(bytes);
		if (isDnsQuery) {
			await forwardVlessUDP(chunk, serverSock, null, addBytes);
			return;
		}
		if (await writeToRemote(chunk)) return;
		if (!isHeaderParsed) {
			chunkBuffer = concatBytes(chunkBuffer, chunk);
			if (chunkBuffer.byteLength < 24) return;
			if (isHeaderParsing) return;
			isHeaderParsing = true;
			reqUUID = extractUUIDFromVless(chunkBuffer);
			if (!reqUUID) {
				serverSock.close();
				return;
			}
			let user = null;
			try {
				user = await env.DB.prepare("SELECT * FROM users WHERE uuid = ?").bind(reqUUID).first();
			} catch (e) {}
			if (isOfflineSet || serverSock.readyState !== WebSocket.OPEN) {
				return;
			}
			if (!user || user.is_active === 0) {
				serverSock.close();
				return;
			}
			if (user.limit_gb && user.used_gb >= user.limit_gb) {
				serverSock.close();
				return;
			}
			if (user.limit_req && user.used_req + (USER_REQ_CACHE.get(user.username) || 0) >= user.limit_req) {
				serverSock.close();
				return;
			}
			if (user.expiry_days && user.created_at) {
				const created = new Date(user.created_at);
				const expiryDate = new Date(created.getTime() + user.expiry_days * 24 * 60 * 60 * 1000);
				if (new Date() > expiryDate) {
					try {
						await env.DB.prepare("UPDATE users SET is_active = 0, last_active = 0 WHERE uuid = ?").bind(reqUUID).run();
					} catch (e) {}
					serverSock.close();
					return;
				}
			}
			validUUID = reqUUID;
			username = user.username;
			isHeaderParsed = true;
			let currentReqs = USER_REQ_CACHE.get(username) || 0;
			USER_REQ_CACHE.set(username, currentReqs + 1);
			let activeCount = ACTIVE_CONNECTIONS_COUNT.get(username) || 0;
			if (user.max_connections && user.max_connections > 0 && activeCount >= user.max_connections) {
				serverSock.close();
				return;
			}
			ACTIVE_CONNECTIONS_COUNT.set(username, activeCount + 1);
			if (activeCount === 0) {
				const setOnlineTask = async () => {
					try {
						const now = Date.now();
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
				const port = (chunkBuffer[offset++] << 8) | chunkBuffer[offset++];
				const addrType = chunkBuffer[offset++];
				let addr = "";
				if (addrType === 1) {
					addr = `${chunkBuffer[offset++]}.${chunkBuffer[offset++]}.${chunkBuffer[offset++]}.${chunkBuffer[offset++]}`;
				} else if (addrType === 2) {
					const domainLen = chunkBuffer[offset++];
					addr = GLOBAL_DECODER.decode(chunkBuffer.slice(offset, offset + domainLen));
					offset += domainLen;
				} else if (addrType === 3) {
					offset += 16;
					addr = "ipv6-unsupported";
				}
				const rawData = chunkBuffer.slice(offset);
				const respHeader = new Uint8Array([chunkBuffer[0], 0]);
				if (cmd === 2) {
					if (port === 53) {
						isDnsQuery = true;
						await forwardVlessUDP(rawData, serverSock, respHeader, addBytes);
					} else {
						serverSock.close();
					}
					return;
				}
				const connectTCP = async (dataPayload = null, useFallback = true) => {
    if (remoteConnWrapper.connectingPromise) {
        await remoteConnWrapper.connectingPromise;
        return;
    }
    const task = (async () => {
        let s = null;
        // ۱. اول پروکسی اختصاصی کاربر رو چک کن
        const userSocks5 = user?.user_socks5 || "";
        if (userSocks5) {
            try {
                s = await connectProxy(userSocks5, addr, port, dataPayload);
            } catch (proxyErr) {
                // اگر پروکسی کاربر خراب بود و fallback مجاز بود، از proxyIP عمومی استفاده کن
                if (useFallback && proxyIP) {
                    s = await connectDirect(proxyIP, port, dataPayload);
                } else {
                    throw proxyErr;
                }
            }
        } else {
            // ۲. در غیر این صورت مثل قبل از proxyIP عمومی استفاده کن
            try {
                s = await connectDirect(addr, port, dataPayload);
            } catch (err) {
                if (useFallback && proxyIP) {
                    s = await connectDirect(proxyIP, port, dataPayload);
                } else {
                    throw err;
                }
            }
        }
        remoteConnWrapper.socket = s;
        s.closed.catch(() => {}).finally(() => closeSocketQuietly(serverSock));
        connectStreams(s, serverSock, respHeader, null, (b) => { addBytes(b); });
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
	const handleWsError = (err) => {
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
	const pushToChain = (task) => {
		wsChain = wsChain.then(task).catch(handleWsError);
	};
	serverSock.addEventListener("message", (event) => {
		if (wsStopped || wsFailed) return;
		const size = event.data.byteLength || 0;
		const nextBytes = wsQueueBytes + size;
		const nextItems = wsQueueItems + 1;
		if (nextBytes > UPSTREAM_QUEUE_MAX_BYTES || nextItems > UPSTREAM_QUEUE_MAX_ITEMS) {
			handleWsError(new Error("ws queue overflow"));
			return;
		}
		wsQueueBytes = nextBytes;
		wsQueueItems = nextItems;
		pushToChain(async () => {
			wsQueueBytes = Math.max(0, wsQueueBytes - size);
			wsQueueItems = Math.max(0, wsQueueItems - 1);
			if (wsFailed) return;
			await processWsMessage(event.data);
		});
	});
	serverSock.addEventListener("close", () => {
		clearInterval(heartbeat);
		closeSocketQuietly(serverSock);
		setOffline();
		if (wsFinished) return;
		wsFinished = true;
		wsStopped = true;
		pushToChain(async () => {
			if (wsFailed) return;
			await upstreamQueue.awaitEmpty();
			releaseRemoteWriter();
		});
	});
	serverSock.addEventListener("error", (err) => {
		handleWsError(err);
	});
	return new Response(null, { status: 101, webSocket: clientSock });
}
async function getCfUsage(env) {
	if (!env.CF_API_TOKEN || !env.CF_ACCOUNT_ID) return { today: 0, total: 0 };
	try {
		const now = new Date();
		const startOfDay = new Date(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()).toISOString();
		const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000).toISOString();
		const q = `query {
      viewer {
        accounts(filter: {accountTag: "${env.CF_ACCOUNT_ID}"}) {
          today: workersInvocationsAdaptive(limit: 10, filter: {datetime_geq: "${startOfDay}"}) {
            sum { requests }
          }
          total: workersInvocationsAdaptive(limit: 10, filter: {datetime_geq: "${thirtyDaysAgo}"}) {
            sum { requests }
          }
        }
      }
    }`;
		const res = await fetch("https://api.cloudflare.com/client/v4/graphql", {
			method: "POST",
			headers: { Authorization: "Bearer " + env.CF_API_TOKEN, "Content-Type": "application/json" },
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
	return parts.length === 4 && parts.every((part) => /^\d{1,3}$/.test(part) && Number(part) >= 0 && Number(part) <= 255);
}
function stripIPv6Brackets(hostname = "") {
	const host = String(hostname || "").trim();
	return host.startsWith("[") && host.endsWith("]") ? host.slice(1, -1) : host;
}
function isIPHostname(hostname = "") {
	const host = stripIPv6Brackets(hostname);
	if (isIPv4(host)) return true;
	if (!host.includes(":")) return false;
	try {
		new URL(`http://[${host}]/`);
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
	const chunks = chunkList.map(convertToUint8Array);
	const total = chunks.reduce((sum, c) => sum + c.byteLength, 0);
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
		if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CLOSING) {
			socket.close();
		}
	} catch (e) {}
}
async function dohQuery(domain, recordType) {
	const cacheKey = `${domain}:${recordType}`;
	if (DNS_CACHE.has(cacheKey)) {
		const cached = DNS_CACHE.get(cacheKey);
		if (Date.now() < cached.expires) return cached.data;
		DNS_CACHE.delete(cacheKey);
	}
	try {
		const typeMap = { A: 1, AAAA: 28 };
		const qtype = typeMap[recordType.toUpperCase()] || 1;
		const encodeDomain = (name) => {
			const parts = name.endsWith(".") ? name.slice(0, -1).split(".") : name.split(".");
			const bufs = [];
			for (const label of parts) {
				const enc = GLOBAL_ENCODER.encode(label);
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
		const response = await fetch(DOH_RESOLVER, {
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
		const parseName = (pos) => {
			const labels = [];
			let p = pos,
				jumped = false,
				endPos = -1,
				safe = 128;
			while (p < buf.length && safe-- > 0) {
				const len = buf[p];
				if (len === 0) {
					if (!jumped) endPos = p + 1;
					break;
				}
				if ((len & 0xc0) === 0xc0) {
					if (!jumped) endPos = p + 2;
					p = ((len & 0x3f) << 8) | buf[p + 1];
					jumped = true;
					continue;
				}
				labels.push(GLOBAL_DECODER.decode(buf.slice(p + 1, p + 1 + len)));
				p += len + 1;
			}
			if (endPos === -1) endPos = p + 1;
			return [labels.join("."), endPos];
		};
		let offset = 12;
		for (let i = 0; i < qdcount; i++) {
			const [, end] = parseName(offset);
			offset = Number(end) + 4;
		}
		const answers = [];
		for (let i = 0; i < ancount && offset < buf.length; i++) {
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
			if (type === 1 && rdlen === 4) {
				data = `${rdata[0]}.${rdata[1]}.${rdata[2]}.${rdata[3]}`;
			} else if (type === 28 && rdlen === 16) {
				const segs = [];
				for (let j = 0; j < 16; j += 2) segs.push(((rdata[j] << 8) | rdata[j + 1]).toString(16));
				data = segs.join(":");
			} else {
				data = Array.from(rdata)
					.map((b) => b.toString(16).padStart(2, "0"))
					.join("");
			}
			answers.push({ name, type, TTL: ttl, data });
		}
		DNS_CACHE.set(cacheKey, { data: answers, expires: Date.now() + DNS_CACHE_TTL });
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
	const settleCompletions = (completions, err = null) => {
		if (!completions) return;
		for (const comp of completions) {
			if (comp) {
				if (err) comp.reject(err);
				else comp.resolve();
			}
		}
	};
	const rejectQueued = (err) => {
		for (let i = head; i < chunks.length; i++) {
			const item = chunks[i];
			if (item && item.completions) settleCompletions(item.completions, err);
		}
	};
	const compact = () => {
		if (head > 32 && head * 2 >= chunks.length) {
			chunks = chunks.slice(head);
			head = 0;
		}
	};
	const resolveIdle = () => {
		if (queuedBytes || draining || !idleResolvers.length) return;
		const resolvers = idleResolvers;
		idleResolvers = [];
		for (const resolve of resolvers) resolve();
	};
	const clear = (err = null) => {
		const closeErr = err || (closed ? new Error(`${name}: queue closed`) : null);
		if (closeErr) {
			rejectQueued(closeErr);
			settleCompletions(activeCompletions, closeErr);
			activeCompletions = null;
		}
		chunks = [];
		head = 0;
		queuedBytes = 0;
		resolveIdle();
	};
	const shift = () => {
		if (head >= chunks.length) return null;
		const item = chunks[head];
		chunks[head++] = undefined;
		queuedBytes -= item.chunk.byteLength;
		compact();
		return item;
	};
	const bundle = () => {
		const first = shift();
		if (!first) return null;
		if (head >= chunks.length || first.chunk.byteLength >= UPSTREAM_BUNDLE_TARGET_BYTES) return first;
		let byteLength = first.chunk.byteLength;
		let end = head;
		let allowRetry = first.allowRetry;
		let completions = first.completions || null;
		while (end < chunks.length) {
			const next = chunks[end];
			const nextLength = byteLength + next.chunk.byteLength;
			if (nextLength > UPSTREAM_BUNDLE_TARGET_BYTES) break;
			byteLength = nextLength;
			allowRetry = allowRetry && next.allowRetry;
			if (next.completions) completions = completions ? completions.concat(next.completions) : next.completions;
			end++;
		}
		if (end === head) return first;
		const output = (bundleBuffer ||= new Uint8Array(UPSTREAM_BUNDLE_TARGET_BYTES));
		output.set(first.chunk);
		let offset = first.chunk.byteLength;
		while (head < end) {
			const next = chunks[head];
			chunks[head++] = undefined;
			queuedBytes -= next.chunk.byteLength;
			output.set(next.chunk, offset);
			offset += next.chunk.byteLength;
		}
		compact();
		return { chunk: output.subarray(0, byteLength), allowRetry, completions };
	};
	const drain = async () => {
		if (draining || closed) return;
		draining = true;
		try {
			for (;;) {
				if (closed) break;
				const item = bundle();
				if (!item) break;
				let writer = getWriter();
				if (!writer) throw new Error(`${name}: remote writer unavailable`);
				const completions = item.completions || null;
				activeCompletions = completions;
				try {
					try {
						await writer.write(item.chunk);
					} catch (err) {
						releaseWriter?.();
						if (!item.allowRetry || typeof retryConnect !== "function") throw err;
						await retryConnect();
						writer = getWriter();
						if (!writer) throw err;
						await writer.write(item.chunk);
					}
					settleCompletions(completions);
				} catch (err) {
					settleCompletions(completions, err);
					throw err;
				} finally {
					if (activeCompletions === completions) activeCompletions = null;
				}
			}
		} catch (err) {
			closed = true;
			clear(err);
			try {
				closeConnection?.(err);
			} catch (_) {}
		} finally {
			draining = false;
			if (!closed && head < chunks.length) queueMicrotask(drain);
			else resolveIdle();
		}
	};
	const enqueue = (data, allowRetry = true, waitForFlush = false) => {
		if (closed) return false;
		if (!getWriter()) return false;
		const chunk = convertToUint8Array(data);
		if (!chunk.byteLength) return true;
		const nextBytes = queuedBytes + chunk.byteLength;
		const nextItems = chunks.length - head + 1;
		if (nextBytes > UPSTREAM_QUEUE_MAX_BYTES || nextItems > UPSTREAM_QUEUE_MAX_ITEMS) {
			closed = true;
			const err = Object.assign(new Error(`${name}: upload queue overflow (${nextBytes}B/${nextItems})`), { isQueueOverflow: true });
			clear(err);
			try {
				closeConnection?.(err);
			} catch (_) {}
			throw err;
		}
		let completionPromise = null;
		let completions = null;
		if (waitForFlush) {
			completions = [];
			completionPromise = new Promise((resolve, reject) => completions.push({ resolve, reject }));
		}
		chunks.push({ chunk, allowRetry, completions });
		queuedBytes = nextBytes;
		if (!draining) queueMicrotask(drain);
		return waitForFlush ? completionPromise.then(() => true) : true;
	};
	return {
		writeAndAwait(data, allowRetry = true) {
			return enqueue(data, allowRetry, true);
		},
		async awaitEmpty() {
			if (!queuedBytes && !draining) return;
			await new Promise((resolve) => idleResolvers.push(resolve));
		},
		clear() {
			closed = true;
			clear();
		},
	};
}
function createDownstreamSender(webSocket, headerData = null) {
	const packetCap = DOWNSTREAM_GRAIN_BYTES;
	const tailBytes = DOWNSTREAM_GRAIN_TAIL_THRESHOLD;
	const lowWaterBytes = Math.max(4096, tailBytes << 3);
	let header = headerData;
	let pendingBuffer = new Uint8Array(packetCap);
	let pendingBytes = 0;
	let flushTimer = null;
	let microtaskQueued = false;
	let generation = 0;
	let scheduledGeneration = 0;
	let waitRounds = 0;
	let flushPromise = null;
	const sendRawChunk = async (chunk) => {
		if (webSocket.readyState !== WebSocket.OPEN) throw new Error("ws.readyState is not open");
		webSocket.send(chunk);
	};
	const attachResponseHeader = (chunk) => {
		if (!header) return chunk;
		const merged = new Uint8Array(header.length + chunk.byteLength);
		merged.set(header, 0);
		merged.set(chunk, header.length);
		header = null;
		return merged;
	};
	const flush = async () => {
		while (flushPromise) await flushPromise;
		if (flushTimer) clearTimeout(flushTimer);
		flushTimer = null;
		microtaskQueued = false;
		if (!pendingBytes) return;
		const output = pendingBuffer.subarray(0, pendingBytes).slice();
		pendingBuffer = new Uint8Array(packetCap);
		pendingBytes = 0;
		waitRounds = 0;
		flushPromise = sendRawChunk(output).finally(() => {
			flushPromise = null;
		});
		return flushPromise;
	};
	const scheduleFlush = () => {
		if (flushTimer || microtaskQueued) return;
		microtaskQueued = true;
		scheduledGeneration = generation;
		queueMicrotask(() => {
			microtaskQueued = false;
			if (!pendingBytes || flushTimer) return;
			if (packetCap - pendingBytes < tailBytes) {
				flush().catch(() => closeSocketQuietly(webSocket));
				return;
			}
			flushTimer = setTimeout(
				() => {
					flushTimer = null;
					if (!pendingBytes) return;
					if (packetCap - pendingBytes < tailBytes) {
						flush().catch(() => closeSocketQuietly(webSocket));
						return;
					}
					if (waitRounds < 2 && (generation !== scheduledGeneration || pendingBytes < lowWaterBytes)) {
						waitRounds++;
						scheduledGeneration = generation;
						scheduleFlush();
						return;
					}
					flush().catch(() => closeSocketQuietly(webSocket));
				},
				Math.max(DOWNSTREAM_GRAIN_SILENT_MS, 1),
			);
		});
	};
	return {
		async sendDirect(data) {
			let chunk = convertToUint8Array(data);
			if (!chunk.byteLength) return;
			chunk = attachResponseHeader(chunk);
			await sendRawChunk(chunk);
		},
		async send(data) {
			let chunk = convertToUint8Array(data);
			if (!chunk.byteLength) return;
			chunk = attachResponseHeader(chunk);
			let offset = 0;
			const totalBytes = chunk.byteLength;
			while (offset < totalBytes) {
				if (!pendingBytes && totalBytes - offset >= packetCap) {
					const sendBytes = Math.min(packetCap, totalBytes - offset);
					const view = offset || sendBytes !== totalBytes ? chunk.subarray(offset, offset + sendBytes) : chunk;
					await sendRawChunk(view);
					offset += sendBytes;
					continue;
				}
				const copyBytes = Math.min(packetCap - pendingBytes, totalBytes - offset);
				pendingBuffer.set(chunk.subarray(offset, offset + copyBytes), pendingBytes);
				pendingBytes += copyBytes;
				offset += copyBytes;
				generation++;
				if (pendingBytes === packetCap || packetCap - pendingBytes < tailBytes) await flush();
				else scheduleFlush();
			}
		},
		flush,
	};
}
async function waitForBackpressure(ws) {
	if (typeof ws.bufferedAmount === "number") {
		while (ws.bufferedAmount > 256 * 1024) {
			await new Promise((r) => setTimeout(r, 100));
		}
	}
}
async function connectStreams(remoteSocket, webSocket, headerData, retryFunc, onBytes) {
	let header = headerData,
		hasData = false,
		reader,
		useBYOB = false;
	const BYOB_LIMIT = 64 * 1024;
	const downstreamSender = createDownstreamSender(webSocket, header);
	header = null;
	try {
		reader = remoteSocket.readable.getReader({ mode: "byob" });
		useBYOB = true;
	} catch (e) {
		reader = remoteSocket.readable.getReader();
	}
	try {
		if (!useBYOB) {
			while (true) {
				await waitForBackpressure(webSocket);
				const { done, value } = await reader.read();
				if (done) break;
				if (!value || value.byteLength === 0) continue;
				hasData = true;
				if (typeof onBytes === "function") onBytes(value.byteLength);
				await downstreamSender.send(value);
			}
		} else {
			let readBuffer = new ArrayBuffer(BYOB_LIMIT);
			while (true) {
				await waitForBackpressure(webSocket);
				const { done, value } = await reader.read(new Uint8Array(readBuffer, 0, BYOB_LIMIT));
				if (done) break;
				if (!value || value.byteLength === 0) continue;
				hasData = true;
				if (typeof onBytes === "function") onBytes(value.byteLength);
				if (value.byteLength >= DOWNSTREAM_GRAIN_BYTES) {
					await downstreamSender.flush();
					await downstreamSender.sendDirect(value);
					readBuffer = new ArrayBuffer(BYOB_LIMIT);
				} else {
					await downstreamSender.send(value);
					readBuffer = value.buffer.byteLength >= BYOB_LIMIT ? value.buffer : new ArrayBuffer(BYOB_LIMIT);
				}
			}
		}
		await downstreamSender.flush();
	} catch (err) {
		closeSocketQuietly(webSocket);
	} finally {
		try {
			reader.cancel();
		} catch (e) {}
		try {
			reader.releaseLock();
		} catch (e) {}
	}
	if (!hasData && retryFunc) await retryFunc();
}
async function buildRaceCandidates(address, port) {
	if (!PRELOAD_RACE_DIAL || isIPHostname(address)) return null;
	const [aRecords, aaaaRecords] = await Promise.all([dohQuery(address, "A"), dohQuery(address, "AAAA")]);
	const ipv4List = [
		...new Set(
			aRecords.flatMap((r) => {
				return r.type === 1 && typeof r.data === "string" && isIPv4(r.data) ? [r.data] : [];
			}),
		),
	];
	const ipv6List = [
		...new Set(
			aaaaRecords.flatMap((r) => {
				return r.type === 28 && typeof r.data === "string" && isIPHostname(r.data) ? [r.data] : [];
			}),
		),
	];
	const limit = Math.max(1, TCP_CONCURRENCY | 0);
	const ipList = ipv4List.length >= limit ? ipv4List.slice(0, limit) : ipv4List.concat(ipv6List.slice(0, limit - ipv4List.length));
	if (ipList.length === 0) return null;
	return ipList.map((hostname, attempt) => ({ hostname, port, attempt, resolvedFrom: address }));
}
async function connectDirect(address, port, initialData = null) {
	const raceCandidates = await buildRaceCandidates(address, port);
	const candidates = raceCandidates || Array.from({ length: TCP_CONCURRENCY }, () => ({ hostname: address, port }));
	const openConnection = async (host, prt) => {
		const socket = connect({ hostname: host, port: prt });
		await Promise.race([socket.opened, new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 1000))]);
		return socket;
	};
	if (candidates.length === 1) {
		const s = await openConnection(candidates[0].hostname, candidates[0].port);
		if (initialData && initialData.byteLength > 0) {
			const w = s.writable.getWriter();
			await w.write(convertToUint8Array(initialData));
			w.releaseLock();
		}
		return s;
	}
	const attempts = candidates.map((c) => openConnection(c.hostname, c.port).then((socket) => ({ socket, candidate: c })));
	let winner = null;
	try {
		winner = await Promise.any(attempts);
		if (initialData && initialData.byteLength > 0) {
			const w = winner.socket.writable.getWriter();
			await w.write(convertToUint8Array(initialData));
			w.releaseLock();
		}
		return winner.socket;
	} finally {
		if (winner) {
			for (const attempt of attempts) {
				attempt
					.then(({ socket }) => {
						if (socket !== winner.socket) {
							try {
								socket.close();
							} catch (e) {}
						}
					})
					.catch(() => {});
			}
		}
	}
}
async function forwardVlessUDP(udpChunk, webSocket, respHeader, onBytes) {
	const requestData = convertToUint8Array(udpChunk);
	try {
		const tcpSocket = connect({ hostname: "8.8.4.4", port: 53 });
		let vlessHeader = respHeader;
		const writer = tcpSocket.writable.getWriter();
		await writer.write(requestData);
		writer.releaseLock();
		await tcpSocket.readable.pipeTo(
			new WritableStream({
				async write(chunk) {
					const response = convertToUint8Array(chunk);
					if (typeof onBytes === "function") onBytes(response.byteLength);
					if (webSocket.readyState !== WebSocket.OPEN) return;
					if (vlessHeader) {
						const merged = new Uint8Array(vlessHeader.length + response.byteLength);
						merged.set(vlessHeader, 0);
						merged.set(response, vlessHeader.length);
						webSocket.send(merged.buffer);
						vlessHeader = null;
					} else {
						webSocket.send(response);
					}
				},
			}),
		);
	} catch (e) {}
}

function trackRequest(env, ctx) {
	GLOBAL_REQ_COUNT++;
	const now = Date.now();
	if (now - GLOBAL_LAST_REQ_WRITE > 60000 && GLOBAL_REQ_COUNT > 0) {
		GLOBAL_LAST_REQ_WRITE = now;
		const countToSave = GLOBAL_REQ_COUNT;
		GLOBAL_REQ_COUNT = 0;
		const task = async () => {
			try {
				const today = new Date().toISOString().split("T")[0];
				
                // اصلاح نوع داده: عدد به عنوان Integer پاس داده می‌شود تا عمل جمع به درستی صورت گیرد
				await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_total', ?) ON CONFLICT(key) DO UPDATE SET value = CAST((CAST(value AS INTEGER) + ?) AS TEXT)").bind(String(countToSave), countToSave).run();
				
				const lastDateRow = await env.DB.prepare("SELECT value FROM settings WHERE key = 'req_last_date'").first();
				if (!lastDateRow || lastDateRow.value !== today) {
					await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_last_date', ?) ON CONFLICT(key) DO UPDATE SET value = ?").bind(today, today).run();
					await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_today', ?) ON CONFLICT(key) DO UPDATE SET value = ?").bind(String(countToSave), String(countToSave)).run();
				} else {
					await env.DB.prepare("INSERT INTO settings (key, value) VALUES ('req_today', ?) ON CONFLICT(key) DO UPDATE SET value = CAST((CAST(value AS INTEGER) + ?) AS TEXT)").bind(String(countToSave), countToSave).run();
				}
			} catch (e) {
                console.error("Track DB Error:", e.message);
            }
		};
		if (ctx) ctx.waitUntil(task());
		else task();
	}
}

function extractUUIDFromVless(data) {
    if (data.byteLength < 17) return null;
    let uuid = "";
    // خواندن مستقیم از بافر با استفاده از LUT_HEX گلوبال
    for (let i = 1; i < 17; i++) {
        uuid += LUT_HEX[data[i]];
        if (i === 4 || i === 6 || i === 8 || i === 10) uuid += "-";
    }
    return uuid;
}
async function handleRailwayWS(request, env, ctx, userUuid) {
    // ۱. فراخوانی کامل اطلاعات کاربر شامل محدودیت‌های ریکوئست
    const user = await env.DB.prepare("SELECT username, is_active, limit_gb, used_gb, limit_req, used_req FROM users WHERE uuid = ?").bind(userUuid).first();
    if (!user || user.is_active === 0) {
        return new Response("Access Denied", { status: 403 });
    }
    
    const username = user.username;
    
    // ۲. چک کردن حجم مصرفی کش شده
    const cachedBytes = GLOBAL_TRAFFIC_CACHE.get(username) || 0;
    if (user.limit_gb && ((user.used_gb || 0) + (cachedBytes / (1024 * 1024 * 1024))) >= user.limit_gb) {
        return new Response("Volume Exceeded", { status: 403 });
    }

    // 🌟 ۳. چک کردن محدودیت ریکوئست و شمارشِ یک اتصالِ جدید
    const cachedReqs = USER_REQ_CACHE.get(username) || 0;
    if (user.limit_req && ((user.used_req || 0) + cachedReqs) >= user.limit_req) {
        return new Response("Request Limit Exceeded", { status: 403 });
    }
    USER_REQ_CACHE.set(username, cachedReqs + 1); // ثبت یک ریکوئست جدید برای کاربر

    // ۴. اضافه شدن شمارنده کاربران آنلاین
    let activeCount = ACTIVE_CONNECTIONS_COUNT.get(username) || 0;
    ACTIVE_CONNECTIONS_COUNT.set(username, activeCount + 1);

    // ۵. موتور همگام‌ساز حافظه با دیتابیس (The Sync Engine)
    function addBytes(bytes) {
        if (bytes <= 0) return;
        let current = GLOBAL_TRAFFIC_CACHE.get(username) || 0;
        GLOBAL_TRAFFIC_CACHE.set(username, current + bytes);
        GLOBAL_LAST_ACTIVE_WRITE.set(username, Date.now());
        
        if (GLOBAL_WRITE_LOCK.get(username)) return;
        
        let lastDbWrite = GLOBAL_LAST_DB_WRITE.get(username) || 0;
        let now = Date.now();
        let thresholdBytes = 10 * 1024 * 1024; // آپدیت دیتابیس به ازای هر ۱۰ مگابایت
        
        if (current >= thresholdBytes || (current > 0 && now - lastDbWrite > 60000)) {
            GLOBAL_WRITE_LOCK.set(username, true);
            let toCommit = GLOBAL_TRAFFIC_CACHE.get(username) || 0;
            let toCommitReq = USER_REQ_CACHE.get(username) || 0;
            
            if (toCommit <= 0 && toCommitReq <= 0) {
                GLOBAL_WRITE_LOCK.set(username, false);
                return;
            }
            
            GLOBAL_TRAFFIC_CACHE.set(username, 0);
            USER_REQ_CACHE.set(username, 0);
            GLOBAL_LAST_DB_WRITE.set(username, now);
            
            let deltaGb = toCommit / (1024 * 1024 * 1024);
            
            env.DB.prepare("UPDATE users SET used_gb = used_gb + ?, used_req = used_req + ? WHERE username = ?")
                .bind(deltaGb, toCommitReq, username)
                .run()
                .catch(e => console.error(e))
                .finally(() => GLOBAL_WRITE_LOCK.set(username, false));
        }
    }

    // ۶. آماده‌سازی هدرها برای ارسال به بک‌اِند
    const newHeaders = new Headers(request.headers);
    let railwayResponse = null;

    // ۷. موتور هوشمند Failover
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
            console.log(`Backend ${backend} failed, trying next...`);
        }
    }

    // ۸. مدیریت خطا در صورت در دسترس نبودن تمام بک‌اندها
    if (!railwayResponse || !railwayResponse.webSocket) {
        let ac = ACTIVE_CONNECTIONS_COUNT.get(username) || 1;
        ACTIVE_CONNECTIONS_COUNT.set(username, Math.max(0, ac - 1));
        return new Response("All Railway Backends Offline", { status: 502 });
    }

    // ۹. استارت تونل
    // ۹. استارت تونل با استفاده از Streams API
    const backendSocket = railwayResponse.webSocket;
    const { 0: clientSocket, 1: localServerSocket } = new WebSocketPair();
    
    backendSocket.accept();
    localServerSocket.accept();

    // --- شروع پیاده‌سازی تونل لوله‌ای (Piping) ---

    // الف) تابع تبدیل رویدادهای وب‌سوکت به استریم استاندارد
    const createWsStream = (ws) => {
        const readable = new ReadableStream({
            start(controller) {
                ws.addEventListener("message", e => controller.enqueue(e.data));
                ws.addEventListener("close", () => controller.close());
                // در صورت بروز خطا، استریم را با ارور می‌بندیم تا حافظه درگیر نماند
                ws.addEventListener("error", e => controller.error(e));
            },
            cancel() { ws.close(); }
        });

        const writable = new WritableStream({
            write(chunk) {
                // عدد 1 معادل WebSocket.OPEN است
                if (ws.readyState === 1) ws.send(chunk);
            },
            close() { ws.close(); },
            abort() { ws.close(); }
        });

        return { readable, writable };
    };

    const localStream = createWsStream(localServerSocket);
    const remoteStream = createWsStream(backendSocket);

    // ب) ساخت کنتور برای شمارش حجم مصرفی در حین عبور داده‌ها (بدون توقف جریان)
    const createTrafficCounter = () => new TransformStream({
        transform(chunk, controller) {
            const bytes = chunk.byteLength || chunk.length || 0;
            addBytes(bytes); // ثبت ترافیک در موتور سینک شما
            controller.enqueue(chunk); // عبور دادن داده به سمت مقصد
        }
    });

    // ج) لوله‌کشی (Piping) داده‌ها با مدیریت خودکار Backpressure
    localStream.readable
        .pipeThrough(createTrafficCounter())
        .pipeTo(remoteStream.writable)
        .catch(() => {}); // بی‌صدا کردن خطاهای طبیعی هنگام قطع اتصال کاربر

    remoteStream.readable
        .pipeThrough(createTrafficCounter())
        .pipeTo(localStream.writable)
        .catch(() => {});

    // --- پایان پیاده‌سازی تونل لوله‌ای ---

    // ۱۰. عملیات پایانی: ذخیره دیتای باقیمانده هنگام قطع شدن کاربر
    let isClosed = false;
    const closeSockets = () => {
        if(isClosed) return;
        isClosed = true;
        
        try { localServerSocket.close(); } catch(e) {}
        try { backendSocket.close(); } catch(e) {}
        
        // محاسبه تعداد اتصالات فعال
        let ac = ACTIVE_CONNECTIONS_COUNT.get(username) || 1;
        ac = Math.max(0, ac - 1);
        ACTIVE_CONNECTIONS_COUNT.set(username, ac);
        
        // 🌟 تغییر کلیدی: فقط اگر ac برابر ۰ باشد، یعنی هیچ اتصال فعالی نداریم
        // و حالا می‌توانیم با خیال راحت دیتابیس را آپدیت و کش را پاک کنیم.
        if (ac === 0) {
            let remainingBytes = GLOBAL_TRAFFIC_CACHE.get(username) || 0;
            let remainingReqs = USER_REQ_CACHE.get(username) || 0;
            
            if (remainingBytes > 0 || remainingReqs > 0) {
                GLOBAL_WRITE_LOCK.set(username, true);
                let deltaGb = remainingBytes / (1024 * 1024 * 1024);
                
                const dbTask = env.DB.prepare("UPDATE users SET used_gb = used_gb + ?, used_req = used_req + ? WHERE username = ?")
                    .bind(deltaGb, remainingReqs, username)
                    .run()
                    .catch(e => console.error("DB Save Error:", e))
                    .finally(() => {
                        // پاک‌سازی کش‌ها فقط در صورتی که همه اتصالات قطع شده باشند
                        GLOBAL_WRITE_LOCK.delete(username);
                        GLOBAL_TRAFFIC_CACHE.delete(username);
                        USER_REQ_CACHE.delete(username);
                    });
                
                if (ctx) ctx.waitUntil(dbTask);
                else dbTask;
            } else {
                // اگر حجمی برای ثبت نبود ولی ac صفر شد، کش‌ها را پاک کن تا حافظه آزاد شود
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

const HTML_TEMPLATES = {
	nginx: `<!DOCTYPE html>
<html lang="fa" dir="rtl" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>دسترسی به پنل</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css" rel="stylesheet" type="text/css" />
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    fontFamily: { sans: ['Vazirmatn', 'sans-serif'] },
                    colors: { amoled: { bg: '#000000', card: '#080b0f', input: '#0d1117', border: '#1c2330' } }
                }
            }
        }
    </script>
</head>
<body class="bg-gray-50 text-gray-900 dark:bg-amoled-bg dark:text-zinc-100 min-h-screen flex items-center justify-center p-4">
    <div class="w-full max-w-md bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-2xl shadow-xl p-8 text-center flex flex-col items-center gap-4">
        <div class="p-4 bg-blue-50 dark:bg-blue-900/20 text-blue-500 rounded-full mb-2">
            <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
        </div>
        <h2 class="text-xl font-bold text-gray-900 dark:text-white">ورود به پنل ماکه‌وصلیم</h2>
        <p class="text-sm text-gray-600 dark:text-gray-400 leading-relaxed mt-2">
            برای ورود به پنل، لطفاً عبارت 
            <span class="inline-block px-2 py-1 bg-gray-100 dark:bg-amoled-input border border-gray-200 dark:border-zinc-800 rounded-md font-mono text-blue-500 font-bold mx-1 shadow-sm" dir="ltr">/panel</span> 
            را به انتهای آدرس مرورگر خود اضافه کنید.
        </p>
        <button onclick="window.location.href='/panel'" class="mt-4 w-full py-2.5 bg-transparent border-2 border-emerald-500 text-emerald-600 hover:bg-emerald-500 hover:text-white dark:border-emerald-400 dark:text-emerald-400 dark:hover:bg-emerald-500 dark:hover:text-white font-medium rounded-xl text-sm transition-colors duration-200 shadow-lg font-bold">
            ورود به پنل
        </button>
    </div>
</body>
</html>`,
	setup: `<!DOCTYPE html>
<html lang="fa" dir="rtl" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>تعریف رمز عبور پنل</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css" rel="stylesheet" type="text/css" />
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    fontFamily: { sans: ['Vazirmatn', 'sans-serif'] },
                    colors: { amoled: { bg: '#000000', card: '#080b0f', input: '#0d1117', border: '#1c2330' } }
                }
            }
        }
    </script>
</head>
<body class="bg-gray-50 text-gray-900 dark:bg-amoled-bg dark:text-zinc-100 min-h-screen flex items-center justify-center p-4">
    <div class="w-full max-w-md bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-2xl shadow-xl p-6">
        <h2 class="text-xl font-bold mb-2 text-center text-blue-600 dark:text-blue-400">تنظیم رمز عبور جدید</h2>
        <p class="text-sm text-gray-500 dark:text-gray-400 text-center mb-6">این اولین ورود شما به پنل مدیریت است. لطفاً رمز عبور خود را تعیین کنید.</p>
        <form onsubmit="handleSetup(event)" class="space-y-4">
            <div>
                <label class="block text-sm font-medium mb-1.5">رمز عبور</label>
                <input type="password" id="password" class="w-full px-3 py-2 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm text-center font-mono" required minlength="4">
            </div>
            <div>
                <label class="block text-sm font-medium mb-1.5">تکرار رمز عبور</label>
                <input type="password" id="confirm-password" class="w-full px-3 py-2 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm text-center font-mono" required minlength="4">
            </div>
            <button type="submit" id="submit-btn" class="w-full py-2.5 bg-transparent border-2 border-emerald-500 text-emerald-600 hover:bg-emerald-500 hover:text-white dark:border-emerald-400 dark:text-emerald-400 dark:hover:bg-emerald-500 dark:hover:text-white font-medium rounded-lg text-sm transition font-bold">ثبت و ورود</button>
        </form>
    </div>
    <div id="toast-container" class="fixed top-5 left-1/2 -translate-x-1/2 z-[9999] flex flex-col gap-2 pointer-events-none"></div>
    <script>
        function showToast(message, type = 'success') {
            const container = document.getElementById('toast-container');
            const toast = document.createElement('div');
            const colors = type === 'error' 
                ? 'bg-red-50 dark:bg-red-900/40 border-red-200 dark:border-red-800 text-red-600 dark:text-red-400' 
                : 'bg-emerald-50 dark:bg-emerald-900/40 border-emerald-200 dark:border-emerald-800 text-emerald-600 dark:text-emerald-400';
            toast.className = 'px-4 py-3 border rounded-xl shadow-lg font-bold text-sm transform transition-all duration-300 -translate-y-full opacity-0 ' + colors;
            toast.innerText = message;
            container.appendChild(toast);
            requestAnimationFrame(() => {
                toast.classList.remove('-translate-y-full', 'opacity-0');
            });
            setTimeout(() => {
                toast.classList.add('-translate-y-full', 'opacity-0');
                setTimeout(() => toast.remove(), 300);
            }, 3000);
        }

        window.alert = function(message) {
            const msgStr = message ? message.toString() : '';
            if (msgStr.includes('خطا') || msgStr.includes('⚠️') || msgStr.includes('❌')) {
                showToast(msgStr, 'error');
            } else {
                showToast(msgStr, 'success');
            }
        };

        async function handleSetup(event) {
            event.preventDefault();
            const password = document.getElementById('password').value;
            const confirmPassword = document.getElementById('confirm-password').value;
            const btn = document.getElementById('submit-btn');
            if (password !== confirmPassword) {
                alert('⚠️ رمز عبور و تکرار آن مطابقت ندارند!');
                return;
            }
            btn.disabled = true;
            btn.innerText = 'در حال ثبت...';
            try {
                const res = await fetch('/api/setup-password', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ password })
                });
                const data = await res.json();
                if (res.ok && data.success) {
                    alert('✅ رمز عبور با موفقیت تنظیم شد. در حال ورود...');
                    setTimeout(() => {
                        window.location.reload();
                    }, 1500);
                } else {
                    alert('خطا: ' + (data.error || 'عملیات ناموفق بود'));
                }
            } catch (err) {
                alert('خطا در ارتباط با سرور');
            } finally {
                btn.disabled = false;
                btn.innerText = 'ثبت و ورود';
            }
        }
    </script>
</body>
</html>`,

	login: `<!DOCTYPE html>
<html lang="fa" dir="rtl" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ورود به پنل ماکه‌وصلیم</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css" rel="stylesheet" type="text/css" />
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    fontFamily: { sans: ['Vazirmatn', 'sans-serif'] },
                    colors: { amoled: { bg: '#000000', card: '#080b0f', input: '#0d1117', border: '#1c2330' } }
                }
            }
        }
    </script>
</head>
<body class="bg-gray-50 text-gray-900 dark:bg-amoled-bg dark:text-zinc-100 min-h-screen flex items-center justify-center p-4">
    <div class="w-full max-w-md bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-2xl shadow-xl p-6">
        <div id="login-section">
            <h2 class="text-xl font-bold mb-6 text-center text-blue-600 dark:text-blue-400">ورود به پنل ماکه‌وصلیم</h2>
            <form onsubmit="handleLogin(event)" class="space-y-4">
                <div>
                    <label class="block text-sm font-medium mb-1.5">رمز عبور</label>
                    <input type="password" id="password" class="w-full px-3 py-2 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm text-center font-mono" required>
                </div>
                <button type="submit" id="submit-btn" class="w-full py-2.5 bg-transparent border-2 border-emerald-500 text-emerald-600 hover:bg-emerald-500 hover:text-white dark:border-emerald-400 dark:text-emerald-400 dark:hover:bg-emerald-500 dark:hover:text-white font-medium rounded-lg text-sm transition font-bold">ورود</button>
            </form>
            <div class="mt-4 text-center">
                <button onclick="toggleRecovery(true)" class="text-xs text-blue-500 hover:text-blue-600 transition font-medium">بازیابی رمز پنل ما‌که‌وصلیم</button>
            </div>
        </div>
        <div id="recovery-section" class="hidden">
            <h2 class="text-xl font-bold mb-4 text-center text-orange-600 dark:text-orange-400">بازیابی رمز پنل ماکه‌وصلیم</h2>
            
            <div class="mb-5 p-3 bg-orange-50 dark:bg-orange-900/20 border border-orange-200 dark:border-orange-800/50 rounded-xl text-xs leading-relaxed text-orange-800 dark:text-orange-300">
                برای احراز هویت و اثبات مالکیت پنل، از طریق دکمه زیر وارد کلودفلر شوید و توکن دریافتی را کپی کرده و در کادر زیر وارد کنید.
                <a href="https://dash.cloudflare.com/profile/api-tokens?permissionGroupKeys=%5B%7B%22key%22%3A%22workers_scripts%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22workers_kv_storage%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22d1%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22account_settings%22%2C%22type%22%3A%22read%22%7D%2C%7B%22key%22%3A%22workers_subdomain%22%2C%22type%22%3A%22edit%22%7D%2C%7B%22key%22%3A%22account_analytics%22%2C%22type%22%3A%22read%22%7D%5D&accountId=*&zoneId=all&name=Zeus-Deployer-Token" target="_blank" class="mt-3 w-full flex items-center justify-center gap-2 py-2 bg-transparent border-2 border-emerald-500 text-emerald-600 hover:bg-emerald-500 hover:text-white dark:border-emerald-400 dark:text-emerald-400 dark:hover:bg-emerald-500 dark:hover:text-white rounded-lg font-bold transition shadow-md">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path></svg>
                    دریافت توکن
                </a>
            </div>

            <form onsubmit="handleRecovery(event)" class="space-y-4">
                <div>
                    <input type="password" id="api-token" placeholder="توکن را وارد کنید" class="w-full px-3 py-2 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-lg focus:outline-none focus:ring-2 focus:ring-orange-500 text-xs text-center font-mono" required>
                </div>
                <div class="flex gap-2 pt-2">
                    <button type="button" onclick="toggleRecovery(false)" class="w-1/3 py-2.5 bg-transparent border-2 border-red-500 text-red-600 hover:bg-red-500 hover:text-white dark:border-red-400 dark:text-red-400 dark:hover:bg-red-500 dark:hover:text-white font-bold rounded-lg text-sm transition shadow-sm">انصراف</button>
                    <button type="submit" id="recover-btn" class="w-2/3 py-2.5 bg-transparent border-2 border-emerald-500 text-emerald-600 hover:bg-emerald-500 hover:text-white dark:border-emerald-400 dark:text-emerald-400 dark:hover:bg-emerald-500 dark:hover:text-white font-medium rounded-lg text-sm transition font-bold">بازیابی رمز پنل ما‌که‌وصلیم</button>
                </div>
            </form>
        </div>
    </div>
    <div id="toast-container" class="fixed top-5 left-1/2 -translate-x-1/2 z-[9999] flex flex-col gap-2 pointer-events-none"></div>
    <script>
        function showToast(message, type = 'success') {
            const container = document.getElementById('toast-container');
            const toast = document.createElement('div');
            const colors = type === 'error' 
                ? 'bg-red-50 dark:bg-red-900/40 border-red-200 dark:border-red-800 text-red-600 dark:text-red-400' 
                : 'bg-emerald-50 dark:bg-emerald-900/40 border-emerald-200 dark:border-emerald-800 text-emerald-600 dark:text-emerald-400';
            toast.className = 'px-4 py-3 border rounded-xl shadow-lg font-bold text-sm transform transition-all duration-300 -translate-y-full opacity-0 ' + colors;
            toast.innerText = message;
            container.appendChild(toast);
            requestAnimationFrame(() => {
                toast.classList.remove('-translate-y-full', 'opacity-0');
            });
            setTimeout(() => {
                toast.classList.add('-translate-y-full', 'opacity-0');
                setTimeout(() => toast.remove(), 300);
            }, 3000);
        }

        window.alert = function(message) {
            const msgStr = message ? message.toString() : '';
            if (msgStr.includes('خطا') || msgStr.includes('⚠️') || msgStr.includes('❌')) {
                showToast(msgStr, 'error');
            } else {
                showToast(msgStr, 'success');
            }
        };

        async function handleLogin(event) {
            event.preventDefault();
            const password = document.getElementById('password').value;
            const btn = document.getElementById('submit-btn');
            btn.disabled = true;
            try {
                const res = await fetch('/api/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ password })
                });
                const data = await res.json();
                if (res.ok && data.success) {
                    window.location.reload();
                } else {
                    alert('❌ رمز عبور اشتباه است');
                }
            } catch (err) {
                alert('خطا در ارتباط با سرور');
            } finally {
                btn.disabled = false;
            }
        }
        function toggleRecovery(show) {
            document.getElementById('login-section').classList.toggle('hidden', show);
            document.getElementById('recovery-section').classList.toggle('hidden', !show);
        }
        async function handleRecovery(event) {
            event.preventDefault();
            const apiToken = document.getElementById('api-token').value;
            const btn = document.getElementById('recover-btn');
            btn.disabled = true;
            btn.innerText = 'در حال بررسی...';
            try {
                const res = await fetch('/api/recover', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ api_token: apiToken })
                });
                const data = await res.json();
                if (res.ok && data.success) {
                    alert('✅ رمز عبور با موفقیت حذف شد. در حال انتقال به صفحه تنظیمات اولیه...');
                    setTimeout(() => {
                        window.location.reload();
                    }, 1500);
                } else {
                    alert('❌ ' + (data.error || 'خطا در تایید اطلاعات'));
                }
            } catch (err) {
                alert('خطا در ارتباط با سرور');
            } finally {
                btn.disabled = false;
                btn.innerText = 'بازیابی رمز پنل ماکه‌وصلیم';
            }
        }
    </script>
</body>
</html>`,

	panel: `
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MK Vaslim Panel</title>
    <script>
        const originalWarn = console.warn;
        console.warn = (...args) => {
            if (typeof args[0] === 'string' && args[0].includes('cdn.tailwindcss.com')) return;
            originalWarn(...args);
        };
    </script>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
    <link href="https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css" rel="stylesheet" type="text/css" />
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    fontFamily: { sans: ['Vazirmatn', 'sans-serif'] },
                    colors: { amoled: { bg: '#000000', card: '#080b0f', input: '#0d1117', border: '#1c2330' } }
                }
            }
        }
    </script>
    <style>
        body { font-family: 'Vazirmatn', sans-serif; }
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        ::-webkit-scrollbar-track {
            background: #f3f4f6; 
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb {
            background: #d1d5db; 
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #9ca3af;
        }
        .dark ::-webkit-scrollbar-track {
            background: #080b0f; 
        }
        .dark ::-webkit-scrollbar-thumb {
            background: #1c2330; 
        }
        .dark ::-webkit-scrollbar-thumb:hover {
            background: #2d3748;
        }
        /* استایل اسکرول‌بار برای مرورگر فایرفاکس */
        * {
            scrollbar-width: thin;
            scrollbar-color: #d1d5db #f6f5f3ff;
        }
        .dark * {
            scrollbar-color: #1c2330 #080b0f;
        }
    </style>
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    fontFamily: { sans: ['Vazirmatn', 'sans-serif'] },
                    colors: {
                        amoled: {
                            bg: '#000000',
                            card: '#080b0f',
                            input: '#0d1117',
                            border: '#1c2330'
                        },
                        // پالت رنگی غروب اضافه شده بدون دستکاری بقیه مقادیر
                        sunset: {
                            bg: '#2b1810',       // قهوه‌ای سوخته متمایل به غروب
                            card: '#3d2314',     // قهوه‌ای گرم و ملایم
                            input: '#4a2b19',    // قهوه‌ای متناسب با اینپوت‌ها
                            border: '#63361c',   // نارنجی-قهوه‌ای تیره برای مرزها
                            text: '#fcead8'      // کرم-پرتقالی بسیار روشن برای متن‌ها
                        }
                    }
                }
            }
        }
    </script>
    
    <style>
        body { font-family: 'Vazirmatn', sans-serif; }
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: #db960bff; border-radius: 4px; }
        ::-webkit-scrollbar-thumb { background: #db960bff; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #9ca3af; }
        .dark ::-webkit-scrollbar-track { background: #080b0f; }
        .dark ::-webkit-scrollbar-thumb { background: #1c2330; }
        .dark ::-webkit-scrollbar-thumb:hover { background: #2d3748; }
        
        /* استایل اسکرول‌بار برای حالت تم غروب */
        .sunset ::-webkit-scrollbar-track { background: #3d2314; }
        .sunset ::-webkit-scrollbar-thumb { background: #63361c; }
        .sunset ::-webkit-scrollbar-thumb:hover { background: #ff7a29; }

        * { scrollbar-width: thin; scrollbar-color: #c3b08aff #c3b08aff; }
        .dark * { scrollbar-color: #1c2330 #080b0f; }
        /* همگام‌سازی فایرفاکس با تم غروب */
        .sunset * { scrollbar-color: #63361c #3d2314; }

        /* کلاس‌های کمکی برای اعمال رنگ‌های غروب زمانی که کلاس sunset فعال است */
        .sunset .bg-white { background-color: #3d2314 !important; }
        .sunset .bg-gray-50 { background-color: #2b1810 !important; }
        .sunset .bg-gray-100 { background-color: #4a2b19 !important; }
        .sunset .text-gray-900, .sunset .text-gray-800, .sunset .text-gray-700 { color: #fcead8 !important; }
        .sunset .text-gray-600, .sunset .text-gray-500 { color: #dca98a !important; }
        .sunset .border-gray-200, .sunset .border-gray-300 { border-color: #63361c !important; }
        .sunset select, .sunset input { background-color: #4a2b19 !important; border-color: #63361c !important; color: #fcead8 !important; }
        .sunset .bg-blue-50 { background-color: #542d16 !important; border-color: #8c4318 !important; color: #ff9d5c !important; }

       /* رفع مشکل سفید شدن ردیف کاربران در حالت هوور (غروب) */
        .sunset tr:hover td, 
        .sunset tr:hover th,
        .sunset td:hover,
        .sunset [class*="hover:bg-"]:hover { 
            background-color: #542d16 !important; 
            color: #61564dff !important; 
        }
        
        /* اطمینان از اینکه متن‌های داخل دکمه‌ها یا المان‌های فرزند در هوور خراب نمی‌شوند */
        .sunset tr:hover * {
            color: inherit !important;
        }
    </style>
    <script>
        (function() {
            // اعمال آنی وضعیت ذخیره شده برای جلوگیری از پرش رنگ زمان لود
            const initialTheme = localStorage.getItem('zeus_panel_theme') || (localStorage.getItem('darkMode') === 'true' ? 'dark' : 'light');
            if (initialTheme === 'sunset') {
                document.documentElement.classList.add('sunset');
            }

            window.addEventListener('load', () => {
                const html = document.documentElement;
                const btn = document.getElementById('theme-toggle');
                if (!btn) return;

                function updateIconDisplay(current) {
                    const sun = document.getElementById('sun-icon');
                    const moon = document.getElementById('moon-icon');
                    const sunset = document.getElementById('sunset-icon');
                    if(!sun || !moon || !sunset) return;

                    // مخفی کردن قطعی همه آیکون‌ها فارغ از کلاس‌های تلویند
                    sun.style.setProperty('display', 'none', 'important');
                    moon.style.setProperty('display', 'none', 'important');
                    sunset.style.setProperty('display', 'none', 'important');

                    // کنترل دقیق نمایش بر اساس نام وضعیت ذخیره شده
                    if (current === 'light') {
                        // در وضعیت روشن: آیکون ماه نمایش داده می‌شود (برای رفتن به حالت شب)
                        moon.style.setProperty('display', 'block', 'important');
                    } else if (current === 'dark') {
                        // در وضعیت تاریک: آیکون خورشید نمایش داده می‌شود (برای رفتن به حالت غروب)
                        sun.style.setProperty('display', 'block', 'important');
                    } else if (current === 'sunset') {
                        // در وضعیت غروب: آیکون دو کوه نمایش داده می‌شود (برای بازگشت به حالت روشن)
                        sunset.style.setProperty('display', 'block', 'important');
                    }
                }

                // مقداردهی اولیه حالت‌ها بر اساس حافظه مرورگر
                let currentTheme = localStorage.getItem('zeus_panel_theme') || (html.classList.contains('dark') ? 'dark' : 'light');
                if (currentTheme === 'sunset') {
                    html.classList.remove('dark');
                    html.classList.add('sunset');
                }
                updateIconDisplay(currentTheme);

                // چرخه سه‌حالته هماهنگ با اسکریپت زئوس
                btn.addEventListener('click', function(e) {
                    setTimeout(() => {
                        let previousTheme = localStorage.getItem('zeus_panel_theme') || (html.classList.contains('dark') ? 'dark' : 'light');

                        // چرخه مهندسی معکوس ثابت: روشن -> تاریک -> غروب -> روشن
                        if (previousTheme === 'light') {
                            html.classList.remove('sunset');
                            localStorage.setItem('zeus_panel_theme', 'dark');
                            localStorage.setItem('darkMode', 'true');
                            updateIconDisplay('dark');
                        } else if (previousTheme === 'dark') {
                            html.classList.remove('dark');
                            html.classList.add('sunset');
                            localStorage.setItem('zeus_panel_theme', 'sunset');
                            localStorage.setItem('darkMode', 'false');
                            updateIconDisplay('sunset');
                        } else if (previousTheme === 'sunset') {
                            html.classList.remove('sunset', 'dark');
                            localStorage.setItem('zeus_panel_theme', 'light');
                            localStorage.setItem('darkMode', 'false');
                            updateIconDisplay('light');
                        }
                    }, 0);
                }, false);
            });
        })();
    </script>
</head>
<body class="bg-gray-50 text-gray-900 dark:bg-amoled-bg dark:text-zinc-100 sunset:bg-sunset-bg sunset:text-sunset-text min-h-screen transition-colors duration-200">
    <header class="border-b border-gray-200 dark:border-amoled-border bg-white dark:bg-amoled-card px-4 py-4">
        <div class="max-w-6xl mx-auto flex flex-col md:flex-row justify-between items-center gap-4">
            <div class="flex flex-row flex-wrap justify-center items-center gap-3 w-full md:w-auto">
                <h1 class="text-lg font-bold flex items-center gap-2" dir="ltr">
                    MK Vaslim Panel 
                    <span id="panel-version" class="text-xs px-2 py-0.5 font-semibold bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 rounded-full">v1.4.10</span>
                </h1>
                <div class="flex items-center gap-3 bg-gray-100 dark:bg-zinc-800/60 px-3 py-1.5 rounded-full border border-gray-200 dark:border-zinc-800/80 shadow-sm flex-shrink-0 w-fit">
                    <a href="ble.ir/join/3eJU64Gzyd" target="_blank" rel="noopener noreferrer" class="text-gray-600 hover:text-gray-800 dark:text-gray-400 dark:hover:text-gray-200 transition-all transform hover:scale-125 duration-200 flex-shrink-0" title="Bale">
                        <svg class="w-5 h-5 group-hover:scale-110 transition" viewBox="0 0 40 40" fill="currentColor">
        <path d="M20,2C10.06,2,2,10.06,2,20s8.06,18,18,18,18-8.06,18-18S29.94,2,20,2Zm7.73,23.36c-1.11,2.58-4,4.64-7.73,4.64a9,9,0,0,1-3.69-.74L11,31l1.74-5.36A9,9,0,0,1,11,20a9,9,0,0,1,9-9c5,0,8.73,3.32,8.73,9A10.23,10.23,0,0,1,27.73,25.36ZM20,13.79A6.21,6.21,0,1,0,26.21,20,6.1,6.1,0,0,0,20,13.79Zm2.26,8.21H17.74a1.13,1.13,0,0,1,0-2.26h4.52a1.13,1.13,0,0,1,0,2.26Z"/>
    </svg>
                    </a>
                    <a href="https://t.me/makevaslim4" target="_blank" rel="noopener noreferrer" class="text-sky-500 hover:text-sky-600 dark:hover:text-sky-400 transition-all transform hover:scale-125 duration-200 flex-shrink-0" title="Telegram">
                        <svg class="w-[22px] h-[22px] flex-shrink-0" viewBox="0 0 24 24" fill="currentColor">
                            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-1.13 7.19-.14.75-.42 1-.68 1.03-.58.05-1.02-.38-1.58-.75-.88-.58-1.38-.94-2.23-1.5-.99-.65-.35-1.01.22-1.59.15-.15 2.71-2.48 2.76-2.69a.2.2 0 00-.05-.18c-.06-.05-.14-.03-.21-.02-.09.02-1.49.94-4.22 2.79-.4.27-.76.41-1.08.4-.36-.01-1.04-.2-1.55-.37-.63-.2-1.12-.31-1.08-.66.02-.18.27-.36.74-.55 2.92-1.27 4.86-2.11 5.83-2.51 2.78-1.16 3.35-1.36 3.73-1.37.08 0 .27.02.39.12.1.08.13.19.14.27-.01.06.01.24 0 .24z"/>
                        </svg>
                    </a>
                </div>
            </div>
            <div class="flex items-center justify-center gap-3 w-full md:w-auto mt-2 md:mt-0">
                <button id="theme-toggle" class="p-2 rounded-lg bg-gray-100 dark:bg-amoled-input sunset:bg-sunset-input border border-gray-200 dark:border-amoled-border sunset:border-sunset-border hover:bg-gray-200 dark:hover:bg-zinc-800 sunset:hover:bg-amber-900/40 transition">
    <svg id="sun-icon" class="w-5 h-5 text-gray-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"></path></svg>
    <svg id="moon-icon" class="w-5 h-5 text-yellow-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364-6.364l-.707.707M6.343 17.657l-.707.707m12.728 0l-.707-.707M6.343 6.343l-.707-.707M14 12a2 2 0 11-4 0 2 2 0 014 0z"></path></svg>
    
    <svg id="sunset-icon" class="w-5 h-5 text-orange-500" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" class="text-orange-500 fill-orange-500/20" d="M12 13a3 3 0 100-6 3 3 0 000 6z" />
        <path stroke-linecap="round" stroke-linejoin="round" d="M12 4V2m0 3v1M8.5 4.5l1 1M15.5 4.5l-1 1" />
        <path stroke-linecap="round" stroke-linejoin="round" d="M2 20l6-8 5 6" />
        <path stroke-linecap="round" stroke-linejoin="round" d="M9 20l5-6.5 6 7.5" />
        <path stroke-linecap="round" stroke-linejoin="round" d="M2 20h20" />
    </svg>
</button>
                <button id="update-toggle" onclick="checkForUpdates(true)" class="p-2 rounded-lg bg-emerald-50 dark:bg-emerald-900/30 border border-emerald-200 dark:border-emerald-800/50 hover:bg-emerald-100 dark:hover:bg-emerald-900/50 transition text-emerald-600 dark:text-emerald-400 relative shadow-sm" title="Update">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 11l3-3m0 0l3 3m-3-3v8m0-13a9 9 0 110 18 9 9 0 010-18z"></path></svg>
                    <span id="update-badge" class="absolute top-0 right-0 w-2.5 h-2.5 bg-red-500 border-2 border-emerald-50 dark:border-emerald-900 rounded-full hidden animate-pulse"></span>
                </button>				
                <button onclick="toggleSettingsModal(true)" class="p-2 rounded-lg bg-gray-100 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border hover:bg-gray-200 dark:hover:bg-zinc-800 transition text-gray-600 dark:text-gray-300 shadow-sm" title="Settings">
                    <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path></svg>
                </button>
                <button onclick="logoutAdmin()" class="p-2 rounded-lg bg-gray-100 dark:bg-amoled-input border border-gray-200 dark:border-amoled-border hover:bg-red-50 dark:hover:bg-red-950/20 transition text-red-600 dark:text-red-400 shadow-sm" title="Logout">
                    <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"></path></svg>
                </button>
            </div>
        </div>
    </header>
    <main class="max-w-6xl mx-auto px-4 py-8 pb-56 md:pb-32">
<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-5 mb-8">
    <div class="bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-2xl p-5 shadow-sm flex flex-col justify-between hover:shadow-md hover:border-indigo-400 dark:hover:border-indigo-500/50 transition duration-300 relative overflow-hidden group">
        <div class="absolute -right-4 -bottom-4 w-24 h-24 bg-indigo-500/10 rounded-full blur-xl group-hover:scale-150 transition duration-500"></div>
        <div class="flex items-center justify-between relative z-10 mb-2">
            <span class="text-sm font-semibold text-gray-500 dark:text-zinc-400 whitespace-nowrap">تعداد کل کاربران</span>
            <div class="p-2 bg-indigo-50 dark:bg-indigo-950/30 text-indigo-600 dark:text-indigo-400 rounded-xl flex-shrink-0">
                <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z"></path></svg>
            </div>
        </div>
        <div class="space-y-1.5 relative z-10 min-w-0 flex-1">
            <div class="text-2xl font-black text-gray-900 dark:text-zinc-100 transition-all" id="stat-total-users">0</div>
            <span class="text-[11px] text-indigo-500 dark:text-indigo-400 flex items-center gap-1 font-medium whitespace-nowrap">
                <span class="w-1.5 h-1.5 bg-indigo-500 rounded-full animate-ping"></span>
                کل کاربران تعریف شده
            </span>
        </div>
    </div>
    <div class="bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-2xl p-5 shadow-sm flex flex-col justify-between hover:shadow-md hover:border-emerald-400 dark:hover:border-emerald-500/50 transition duration-300 relative overflow-hidden group">
        <div class="absolute -right-4 -bottom-4 w-24 h-24 bg-emerald-500/10 rounded-full blur-xl group-hover:scale-150 transition duration-500"></div>
        <div class="flex items-center justify-between relative z-10 mb-2">
            <span class="text-sm font-semibold text-gray-500 dark:text-zinc-400 whitespace-nowrap">کاربران فعال (آنلاین)</span>
            <div class="p-2 bg-emerald-50 dark:bg-emerald-950/30 text-emerald-600 dark:text-emerald-400 rounded-xl flex-shrink-0">
                <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
            </div>
        </div>
        <div class="space-y-1.5 relative z-10 min-w-0 flex-1">
            <div class="text-2xl font-black text-emerald-600 dark:text-emerald-400 transition-all" id="stat-active-users">0</div>
            <span class="text-[11px] text-emerald-500 dark:text-emerald-400 flex items-center gap-1 font-medium whitespace-nowrap">
                <span class="w-2 h-2 bg-emerald-500 rounded-full animate-pulse"></span>
                متصل در این لحظه
            </span>
        </div>
    </div>
	<div id="card-cf-requests" class="bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-2xl p-5 shadow-sm flex flex-col justify-between hover:shadow-md hover:border-orange-400 dark:hover:border-orange-500/50 transition duration-300 relative overflow-hidden group">
	    <div class="absolute -right-4 -bottom-4 w-24 h-24 bg-orange-500/10 rounded-full blur-xl group-hover:scale-150 transition duration-500"></div>
	    <div class="flex items-center justify-between relative z-10 mb-2">
	        <span class="text-sm font-semibold text-gray-500 dark:text-zinc-400 whitespace-nowrap">ریکوئست‌های روزانه</span>
	        <div class="p-2 bg-orange-50 dark:bg-orange-950/30 text-orange-600 dark:text-orange-400 rounded-xl flex-shrink-0">
	            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 15a4 4 0 004 4h9a5 5 0 10-.1-9.999 5.002 5.002 0 10-9.78 2.096A4.001 4.001 0 003 15z"></path></svg>
	        </div>
	    </div>
	    <div class="space-y-2 relative z-10 min-w-0 flex-1">
	        <div class="flex items-center gap-1">
	            <span class="text-2xl font-black text-orange-600 dark:text-orange-400 transition-all" id="stat-cf-requests">0</span>
	            <span class="text-xs font-bold text-gray-400 mr-1">/ 100k</span>
	            <button id="cf-warning-btn" onclick="openUsageWarning()" class="hidden flex items-center justify-center w-5 h-5 bg-red-100 dark:bg-red-900/40 text-red-600 dark:text-red-400 rounded-full font-bold text-xs animate-bounce shadow-sm border border-red-300 dark:border-red-700 mr-2">!</button>
	        </div>
	        <div class="w-full bg-gray-100 dark:bg-zinc-800 rounded-full h-1.5 mt-1">
	            <div id="stat-cf-progress" class="bg-orange-500 h-1.5 rounded-full transition-all duration-500" style="width: 0%"></div>
	        </div>
	        <span class="text-[11px] text-orange-500 dark:text-orange-400 flex items-center justify-between font-medium whitespace-nowrap mt-1">
	            <span>Total: <span id="stat-cf-total">0</span></span>
	            <span dir="ltr">Cloudflare</span>
	        </span>
	    </div>
	</div>
    <div class="bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-2xl p-5 shadow-sm flex flex-col justify-between hover:shadow-md hover:border-blue-400 dark:hover:border-blue-500/50 transition duration-300 relative overflow-hidden group">
        <div class="absolute -right-4 -bottom-4 w-24 h-24 bg-blue-500/10 rounded-full blur-xl group-hover:scale-150 transition duration-500"></div>
        <div class="flex items-center justify-between relative z-10 mb-2">
            <span class="text-sm font-semibold text-gray-500 dark:text-zinc-400 whitespace-nowrap">حجم مصرفی (۳۰ روز)</span>
            <div class="p-2 bg-blue-50 dark:bg-blue-950/30 text-blue-600 dark:text-blue-400 rounded-xl flex-shrink-0">
                <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"></path></svg>
            </div>
        </div>
        <div class="space-y-1.5 relative z-10 min-w-0 flex-1">
            <div class="text-2xl font-black text-blue-600 dark:text-blue-400 transition-all whitespace-nowrap" id="stat-total-usage">0 GB</div>
            <span class="text-[11px] text-blue-500 dark:text-blue-400 flex items-center gap-1 font-medium whitespace-nowrap">
                <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M9 19l3 3m0 0l3-3m-3 3V10"></path></svg>
                مصرف کل کاربران
            </span>
        </div>
    </div>
    <div class="bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-2xl p-5 shadow-sm flex flex-col justify-between hover:shadow-md hover:border-amber-400 dark:hover:border-amber-500/50 transition duration-300 relative overflow-hidden group">
        <div class="absolute -right-4 -bottom-4 w-24 h-24 bg-amber-500/10 rounded-full blur-xl group-hover:scale-150 transition duration-500"></div>
        <div class="flex items-center justify-between relative z-10 mb-2">
            <span class="text-sm font-semibold text-gray-500 dark:text-zinc-400 whitespace-nowrap">پر مصرف‌ترین کاربر</span>
            <div class="p-2 bg-amber-50 dark:bg-amber-950/30 text-amber-600 dark:text-amber-400 rounded-xl flex-shrink-0">
                <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"></path></svg>
            </div>
        </div>
        <div class="space-y-1.5 relative z-10 min-w-0 flex-1">
            <div class="text-xl font-black text-amber-600 dark:text-amber-400 transition-all truncate max-w-[150px]" id="stat-top-user">-</div>
            <span class="text-[11px] text-amber-500 dark:text-amber-400 flex items-center gap-1 font-medium whitespace-nowrap" id="stat-top-user-usage">۰ GB مصرف شده</span>
        </div>
    </div>
</div>
        <div id="loading-state" class="text-center py-12">
            <span class="text-gray-500 dark:text-gray-400">در حال بارگذاری کاربران...</span>
        </div>
        <div class="mb-6 flex flex-col md:flex-row gap-4 justify-between items-center bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-2xl p-4 shadow-sm">
            <!-- Search Box -->
            <div class="relative w-full md:w-80">
                <input type="text" id="search-input" oninput="filterAndRenderUsers()" placeholder="جستجوی نام کاربری یا UUID..." class="w-full pl-3 pr-9 py-2.5 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm">
                <div class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none text-gray-400">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path></svg>
                </div>
            </div>
            <!-- Filters & Sorting -->
            <div class="flex flex-wrap items-center gap-3 w-full md:w-auto">
                <!-- Status Filter -->
                <select id="filter-status" onchange="filterAndRenderUsers()" class="px-3 py-2.5 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-700 dark:text-zinc-300 cursor-pointer">
                    <option value="all">🔍 همه وضعیت‌ها</option>
                    <option value="active">✅ فعال</option>
                    <option value="inactive">❌ غیرفعال</option>
                    <option value="online">⚡ آنلاین</option>
                    <option value="offline">💤 آفلاین</option>
                    <option value="expired">⏳ منقضی شده / تمام شده</option>
                </select>
                <!-- Sorting -->
                <select id="sort-users" onchange="filterAndRenderUsers()" class="px-3 py-2.5 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-700 dark:text-zinc-300 cursor-pointer">
                    <option value="newest">📅 جدیدترین</option>
                    <option value="name">🔤 نام کاربری (الفبا)</option>
                    <option value="usage-desc">📊 بیشترین مصرف</option>
                    <option value="usage-asc">📈 کمترین مصرف</option>
                    <option value="expiry-asc">⏳ کمترین زمان باقی‌مانده</option>
                </select>
            </div>
        </div>
		<div class="flex items-center justify-between mb-4">
			<h2 class="text-lg font-bold text-gray-800 dark:text-zinc-200">لیست کاربران</h2>
			<button onclick="openCreateModal()" class="p-2 rounded-lg bg-blue-50 dark:bg-blue-900/30 border-2 border-blue-200 dark:border-blue-800/50 hover:bg-blue-100 dark:hover:bg-blue-900/50 transition-all duration-300 text-blue-600 dark:text-blue-400 shadow-sm hover:shadow hover:scale-110">
    			<svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M12 4v16m8-8H4"></path></svg>
			</button>
		</div>
        <div id="users-table-container" class="hidden overflow-x-auto border border-gray-200 dark:border-amoled-border rounded-xl bg-white dark:bg-amoled-card">
            <table class="w-full text-right border-collapse">
                <thead>
                    <tr class="bg-gray-100 dark:bg-zinc-900/50 border-b border-gray-200 dark:border-amoled-border text-xs text-gray-500 dark:text-gray-400 text-center">
                        <th class="p-2 w-10 text-center"><input type="checkbox" id="select-all-users" onchange="toggleSelectAllUsers(this)" class="w-5 h-5 rounded-md border-2 border-gray-300 dark:border-zinc-700 text-blue-600 bg-white dark:bg-zinc-800 checked:bg-blue-600 checked:border-blue-600 focus:ring-blue-500/50 focus:ring-offset-0 transition-all duration-200 cursor-pointer hover:scale-105 active:scale-95"></th>
                        <th class="p-4">نام کاربر و عملیات</th>
                        <th class="p-2 border-r border-gray-200 dark:border-zinc-800">لینک ساب</th>
                        <th class="p-2 border-r border-gray-200 dark:border-zinc-800">پروتکل</th>
                        <th class="p-2 border-r border-gray-200 dark:border-zinc-800">پورت</th>
                        <th class="p-2 border-r border-gray-200 dark:border-zinc-800">حجم</th>
                        <th class="p-2 border-r border-gray-200 dark:border-zinc-800">ریکوئست</th>
                        <th class="p-2 border-r border-gray-200 dark:border-zinc-800">زمان</th>
                        <th class="p-2 border-r border-gray-200 dark:border-zinc-800">کاربران آنلاین</th>
                        <th class="p-2 border-r border-gray-200 dark:border-zinc-800">تاریخ ساخت</th>
                    </tr>
                </thead>
                <tbody id="users-tbody" class="divide-y divide-gray-150 dark:divide-amoled-border text-sm"></tbody>
            </table>
        </div>
        <div id="empty-state" class="hidden p-8 border border-dashed border-gray-300 dark:border-amoled-border rounded-2xl text-center">
            <p class="text-gray-500 dark:text-gray-400">کاربری وجود ندارد. برای ساخت اولین کاربر روی دکمه «افزودن کاربر جدید» کلیک کنید.</p>
        </div>
    </main>
<div id="path-warning-modal" class="fixed inset-0 z-[70] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out">
    <div class="w-full max-w-md bg-white dark:bg-amoled-card border border-red-500/50 rounded-3xl shadow-2xl overflow-hidden p-6 text-center transition-all transform duration-300 opacity-0 scale-95 ease-out">
        <div class="inline-flex items-center justify-center w-16 h-16 rounded-full bg-red-100 dark:bg-red-900/30 text-red-500 mb-4 shadow-inner">
            <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>
        </div>
        <h3 class="font-black text-xl text-gray-900 dark:text-white mb-2">تغییر مهم در ساختار کانفیگ‌ها</h3>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-6 leading-relaxed font-medium">
            به دلیل ارتقای امنیت و تغییر مسیر (Path) اتصال ، کانفیگ‌های قبل از نسخه 1.3.4 غیرفعال شده‌اند. درصورت عدم اتصال لطفاً ساب خود را بروزرسانی کنید .
        </p>
        <button onclick="closePathWarning()" class="w-full py-3.5 bg-red-600 hover:bg-red-700 text-white font-black rounded-xl text-sm transition duration-300 shadow-lg shadow-red-500/25">
            متوجه شدم، کانفیگ‌های جدید را می‌گیرم 
        </button>
    </div>
</div>
<div id="usage-warning-modal" class="fixed inset-0 z-[80] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out">
    <div class="w-full max-w-md bg-white dark:bg-amoled-card border border-orange-500/50 rounded-3xl shadow-2xl overflow-hidden p-6 text-center transition-all transform duration-300 opacity-0 scale-95 ease-out">
        <div class="inline-flex items-center justify-center w-16 h-16 rounded-full bg-orange-100 dark:bg-orange-900/30 text-orange-500 mb-4 shadow-inner">
            <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>
        </div>
        <h3 class="font-black text-xl text-gray-900 dark:text-white mb-2">هشدار محدودیت درخواست روزانه</h3>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-6 leading-relaxed font-medium">
            درخواست‌های روزانه کلودفلر شما از ۹۰,۰۰۰ عبور کرده است. در صورت عبور از محدودیت رایگان ۱۰۰,۰۰۰ درخواست، دسترسی به پنل و اتصالات تا ساعت ۳:۳۰ بامداد (به وقت ایران) قطع خواهد شد.
        </p>
        <button onclick="closeUsageWarning()" class="w-full py-3.5 bg-orange-600 hover:bg-orange-700 text-white font-black rounded-xl text-sm transition duration-300 shadow-lg shadow-orange-500/25">
            متوجه شدم
        </button>
    </div>
</div>
    <div id="user-modal" class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 opacity-0 pointer-events-none transition-opacity duration-200 ease-out">
        <div id="user-modal-card" class="w-full max-w-xl bg-white dark:bg-zinc-950 border border-gray-200 dark:border-zinc-800 rounded-2xl shadow-xl overflow-hidden transition-[opacity,transform] duration-200 opacity-0 scale-95 ease-out flex flex-col max-h-[90vh] transform-gpu" style="will-change: transform, opacity;">
            <div class="px-6 py-4 border-b border-gray-150 dark:border-zinc-800/80 flex justify-between items-center bg-gray-50/50 dark:bg-zinc-900/30">
                <div class="flex items-center gap-2">
                    <div class="w-2.5 h-2.5 rounded-full bg-blue-500"></div>
                    <h3 id="modal-title" class="font-bold text-gray-900 dark:text-zinc-100 text-base">ایجاد کاربر جدید</h3>
                </div>
                <button onclick="toggleModal(false)" class="p-1 rounded-lg hover:bg-gray-150 dark:hover:bg-zinc-800/60 text-gray-400 hover:text-gray-650 dark:hover:text-zinc-200 transition">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
                </button>
            </div>
            <form id="create-user-form" class="p-6 space-y-5 overflow-y-auto flex-1 overscroll-contain" style="-webkit-overflow-scrolling: touch; transform: translate3d(0,0,0); will-change: scroll-position, transform;" onsubmit="handleFormSubmit(event)">
                <div class="space-y-4">
                    <div>
                        <label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 mb-2 uppercase tracking-wider">نام کاربری</label>
                        <div class="relative">
                            <span class="absolute inset-y-0 right-0 flex items-center pr-3.5 pointer-events-none text-gray-400">
                                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"></path></svg>
                            </span>
                            <input type="text" id="input-name" placeholder="ali" class="w-full pl-3 pr-10 py-2.5 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-sm font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition" required>
                        </div>
                    </div>
                    <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
                        <div>
                            <label class="block text-[10px] sm:text-xs font-bold text-gray-500 dark:text-zinc-400 mb-2 uppercase tracking-wider">حجم (GB)</label>
                            <div class="relative">
                                <span class="absolute inset-y-0 right-0 flex items-center pr-3.5 pointer-events-none text-gray-400">
                                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"></path></svg>
                                </span>
                                <input type="number" id="input-limit" min="0" step="any" placeholder="نامحدود" class="w-full pl-3 pr-10 py-2.5 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-sm font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition">
                            </div>
                        </div>
                        <div>
                            <label class="block text-[10px] sm:text-xs font-bold text-gray-500 dark:text-zinc-400 mb-2 uppercase tracking-wider">اعتبار (روز)</label>
                            <div class="relative">
                                <span class="absolute inset-y-0 right-0 flex items-center pr-3.5 pointer-events-none text-gray-400">
                                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
                                </span>
                                <input type="number" id="input-expiry" min="0" placeholder="نامحدود" class="w-full pl-3 pr-10 py-2.5 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-sm font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition">
                            </div>
                        </div>
                        <div>
                            <label class="block text-[10px] sm:text-xs font-bold text-gray-500 dark:text-zinc-400 mb-2 uppercase tracking-wider">سقف ریکوئست</label>
                            <div class="relative">
                                <span class="absolute inset-y-0 right-0 flex items-center pr-3.5 pointer-events-none text-gray-400">
                                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
                                </span>
                                <input type="number" id="input-req-limit" min="0" placeholder="نامحدود" class="w-full pl-3 pr-10 py-2.5 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-sm font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition">
                            </div>
                        </div>
                        <div>
                            <label class="block text-[10px] sm:text-xs font-bold text-gray-500 dark:text-zinc-400 mb-2 uppercase tracking-wider">کاربر همزمان</label>
                            <div class="relative">
                                <span class="absolute inset-y-0 right-0 flex items-center pr-3.5 pointer-events-none text-gray-400">
                                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z"></path></svg>
                                </span>
                                <input type="number" id="input-max-connections" min="0" placeholder="نامحدود" class="w-full pl-3 pr-10 py-2.5 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-sm font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition">
                            </div>
                        </div>
                    </div>
                </div>
                <div class="pt-2 border-t border-gray-100 dark:border-zinc-900">
                    <label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 mb-3 uppercase tracking-wider">پورت‌های اتصال (انتخاب چندگانه)</label>
                    <div class="space-y-4">
                        <div class="p-4 bg-gray-50/50 dark:bg-zinc-900/20 border border-gray-200/60 dark:border-zinc-800 rounded-2xl shadow-sm">
                            <div class="flex items-center gap-1.5 mb-3">
                                <span class="flex h-2 w-2 rounded-full bg-blue-500 shadow-sm"></span>
                                <span class="text-xs font-bold text-blue-600 dark:text-blue-400">🔒 پورت‌های امن (TLS)</span>
                            </div>
                            <div class="grid grid-cols-3 sm:grid-cols-4 gap-2" id="tls-ports-list">
                                <!-- Filled dynamically -->
                            </div>
                        </div>
                        <div class="p-4 bg-gray-50/50 dark:bg-zinc-900/20 border border-gray-200/60 dark:border-zinc-800 rounded-2xl shadow-sm">
                            <div class="flex items-center gap-1.5 mb-3">
                                <span class="flex h-2 w-2 rounded-full bg-amber-500 shadow-sm"></span>
                                <span class="text-xs font-bold text-amber-600 dark:text-amber-400">🔓 پورت‌های معمولی (Non-TLS)</span>
                            </div>
                            <div class="grid grid-cols-3 sm:grid-cols-4 gap-2" id="nontls-ports-list">
                                <!-- Filled dynamically -->
                            </div>
                        </div>
                    </div>
                </div>
                <div class="pt-4 border-t border-gray-100 dark:border-zinc-900 space-y-4">
					<div>
    					<div class="flex items-center justify-between mb-2">
        					<label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 uppercase tracking-wider">آیپی تمیز کلودفلر (اختیاری)</label>
        					<button type="button" onclick="openIpSelectorModal()" class="px-2.5 py-1 bg-indigo-50 dark:bg-indigo-900/30 text-indigo-600 dark:text-indigo-400 hover:bg-indigo-100 dark:hover:bg-indigo-900/50 rounded-lg text-xs font-bold transition border border-indigo-200 dark:border-indigo-800">مخزن آیپی تمیز</button>
    					</div>
    					<textarea id="input-ips" rows="2" placeholder="104.16.0.1" class="w-full px-3 py-2.5 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-mono text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition resize-none"></textarea>
					</div>
                    <div>
                        <label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 mb-2 uppercase tracking-wider">شبیه‌ساز اثر انگشت مرورگر (Fingerprint)</label>
                        <div class="relative">
                            <select id="fingerprint-select" class="w-full px-3 py-2.5 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-semibold text-gray-700 dark:text-zinc-300 cursor-pointer appearance-none">
                                <option value="chrome" selected>🌐 Chrome  (پیش‌فرض)</option>
                                <option value="firefox">🦊 Firefox</option>
                                <option value="safari">🧭 Safari</option>
                                <option value="ios">📱 iOS Device</option>
                                <option value="android">🤖 Android Device</option>
                                <option value="edge">🌀 Microsoft Edge</option>
                                <option value="360">🔒 360 Browser</option>
                                <option value="qq">💬 QQ Browser</option>
                                <option value="random">🎲 Random (اتفاقی)</option>
                                <option value="randomized">🎭 Randomized (پویا)</option>
                            </select>
                            <div class="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3 text-gray-500 dark:text-zinc-400">
                                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
                            </div>
                        </div>
                    </div>
                </div>
<!-- بخش پروکسی اختصاصی -->
<div class="pt-4 border-t border-gray-100 dark:border-zinc-900 space-y-4">
    <div class="flex items-center justify-between">
        <div class="flex items-center gap-2">
            <svg class="w-4 h-4 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>
            <label class="text-xs font-bold text-gray-500 dark:text-zinc-400 uppercase tracking-wider">پروکسی اختصاصی (SOCKS5/HTTP)</label>
        </div>
        <label class="relative inline-flex items-center cursor-pointer select-none">
            <input type="checkbox" id="user-proxy-mode-toggle" onchange="toggleUserProxyMode(this.checked)" class="sr-only peer">
            <div class="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"></div>
        </label>
    </div>
    <div id="user-socks5-container" class="transition-opacity duration-300 opacity-50 pointer-events-none">
        <input type="text" id="user-socks5-input" placeholder="socks5://user:pass@ip:port  یا http://user:pass@ip:port" dir="ltr" class="w-full px-3 py-2.5 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-mono text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition" disabled>
        <div class="w-full text-center">
            <span id="test-user-proxy-result" class="inline-block mt-2 text-[11px] font-bold transition-colors break-words leading-relaxed empty:hidden"></span>
        </div>
        <div class="mt-2 flex items-center justify-between w-full gap-2">
            <button type="button" onclick="testUserSocksProxy()" id="test-user-proxy-btn" class="flex-1 text-center text-[11px] bg-sky-50 dark:bg-sky-900/30 text-sky-600 dark:text-sky-400 py-1.5 rounded border border-sky-200 dark:border-sky-800 hover:bg-sky-100 dark:hover:bg-sky-900/50 transition font-bold shadow-sm">تست پروکسی</button>
        </div>
    </div>
</div>
                <div class="pt-4 flex gap-3">
                    <button type="button" onclick="toggleModal(false)" class="flex-1 py-3 bg-gray-100 hover:bg-gray-200 dark:bg-zinc-800 dark:hover:bg-zinc-700/80 text-gray-700 dark:text-zinc-300 font-bold rounded-xl text-sm transition duration-200">انصراف</button>
                    <button type="submit" id="submit-btn" class="flex-1 py-3 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700 text-white font-bold rounded-xl text-sm transition duration-200 shadow-md shadow-blue-500/10 hover:shadow-lg">ایجاد کاربر</button>
                </div>
            </form>
        </div>
    </div>
<div id="ip-selector-modal" class="fixed inset-0 z-[60] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out">
    <div class="w-full max-w-sm bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-2xl shadow-xl overflow-hidden transition-all transform duration-300 opacity-0 scale-95 ease-out">
        <div class="px-6 py-4 border-b border-gray-150 dark:border-amoled-border flex justify-between items-center bg-gray-50 dark:bg-zinc-900/50">
            <h3 class="font-bold text-gray-900 dark:text-zinc-100 text-sm">مخزن آیپی تمیز</h3>
            <button type="button" onclick="toggleIpSelectorModal(false)" class="text-gray-400 hover:text-gray-600 dark:hover:text-zinc-200">
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
            </button>
        </div>
        <div class="p-6 space-y-4">
            <div id="ip-loading-state" class="text-center text-sm text-gray-500 dark:text-zinc-400 hidden">
                Loading IPs...
            </div>
            <div id="ip-selection-form" class="space-y-4">
                <div>
                    <label class="block text-xs font-medium mb-1.5 text-gray-700 dark:text-zinc-300">اوپراتور</label>
                    <select id="ip-operator-select" class="w-full px-3 py-2.5 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-xl text-xs focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-700 dark:text-zinc-300 cursor-pointer">
                        <option value="all">All</option>
                    </select>
                </div>
                <div>
                    <label class="block text-xs font-medium mb-1.5 text-gray-700 dark:text-zinc-300">تعداد</label>
                    <input type="number" id="ip-count-input" min="1" value="10" dir="ltr" class="w-full px-3 py-2.5 bg-gray-50 dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500 text-xs font-mono text-center">
                </div>
            </div>
            <div class="pt-4 flex gap-3">
                <button type="button" onclick="toggleIpSelectorModal(false)" class="flex-1 py-2 bg-gray-100 hover:bg-gray-200 dark:bg-zinc-800 dark:hover:bg-zinc-700 font-medium rounded-xl text-xs transition">لغو</button>
                <button type="button" onclick="applySelectedIps()" class="flex-1 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-medium rounded-xl text-xs transition">دریافت</button>
            </div>
        </div>
    </div>
</div>
    <div id="settings-modal" class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out">
        <div class="w-full max-w-md bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-2xl shadow-xl overflow-hidden transition-all transform duration-300 opacity-0 scale-95 ease-out flex flex-col max-h-[90vh]">
            <div class="px-6 py-4 border-b border-gray-150 dark:border-amoled-border flex justify-between items-center bg-gray-50 dark:bg-zinc-900/50">
                <h3 class="font-bold text-gray-900 dark:text-zinc-100">تنظیمات پنل ما‌که‌وصلیم</h3>
                <button onclick="toggleSettingsModal(false)" class="text-gray-400 hover:text-gray-600 dark:hover:text-zinc-200">
                    <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
                </button>
            </div>
            <div class="p-6 space-y-4 overflow-y-auto flex-1 overscroll-contain">
                <div>
                    <label class="block text-sm font-medium mb-1.5 text-gray-700 dark:text-zinc-300">موقعیت جغرافیایی پروکسی (Cloudflare)</label>
                    <div class="relative">
                        <select id="location-select" class="w-full pl-8 pr-3 py-2.5 bg-white dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 text-gray-700 dark:text-zinc-200 cursor-pointer appearance-none">
                            <option value="">در حال بارگذاری...</option>
                        </select>
                        <div class="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3 text-gray-500 dark:text-zinc-400">
                            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
                        </div>
                    </div>
                </div>
                <div class="grid grid-cols-2 gap-4 pt-2 border-t border-gray-100 dark:border-zinc-800">
                    <div>
                        <label class="block text-sm font-medium mb-1.5 text-gray-700 dark:text-zinc-300">Fragment Length</label>
                        <input type="text" id="frag-length" placeholder="50-100" class="w-full px-3 py-2.5 bg-white dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm text-center font-mono" dir="ltr">
                    </div>
                    <div>
                        <label class="block text-sm font-medium mb-1.5 text-gray-700 dark:text-zinc-300">Fragment Interval</label>
                        <input type="text" id="frag-interval" placeholder="3-5" class="w-full px-3 py-2.5 bg-white dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm text-center font-mono" dir="ltr">
                    </div>
                </div>
                <!-- Change Password Section -->
                <div class="pt-4 border-t border-gray-100 dark:border-zinc-800">
                    <h4 class="text-sm font-bold mb-3 text-gray-800 dark:text-zinc-200">🔒 تغییر رمز عبور مدیریت</h4>
                    <div class="space-y-3">
                        <div>
                            <label class="block text-[11px] text-gray-500 dark:text-gray-400 font-medium mb-1">رمز عبور فعلی</label>
                            <input type="password" id="change-pwd-current" class="w-full px-3 py-2 bg-white dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-xs font-mono text-center">
                        </div>
                        <div>
                            <label class="block text-[11px] text-gray-500 dark:text-gray-400 font-medium mb-1">رمز عبور جدید</label>
                            <input type="password" id="change-pwd-new" class="w-full px-3 py-2 bg-white dark:bg-amoled-input border border-gray-300 dark:border-amoled-border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 text-xs font-mono text-center">
                        </div>
                        <button type="button" onclick="changeAdminPassword()" id="change-pwd-btn" class="w-full py-2 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700 text-white font-semibold rounded-lg text-xs transition-all shadow-sm">تغییر رمز عبور</button>
                    </div>
                </div>
                <!-- Backup & Restore Section -->
                <div class="pt-4 border-t border-gray-100 dark:border-zinc-800">
                    <h4 class="text-sm font-bold mb-3 text-gray-800 dark:text-zinc-200">💾 پشتیبان‌گیری و بازیابی</h4>
                    <div class="grid grid-cols-2 gap-3">
                        <button type="button" onclick="exportUsersBackup()" class="py-2.5 bg-indigo-50 dark:bg-indigo-950/20 text-indigo-600 dark:text-indigo-400 hover:bg-indigo-100 dark:hover:bg-indigo-900/30 border border-indigo-200 dark:border-indigo-900/50 rounded-xl text-xs font-bold transition flex items-center justify-center gap-1.5 shadow-sm">
                            📤 پشتیبان گیری
                        </button>
                        <button type="button" onclick="triggerImportBackup()" class="py-2.5 bg-emerald-50 dark:bg-emerald-950/20 text-emerald-600 dark:text-emerald-400 hover:bg-emerald-100 dark:hover:bg-emerald-900/30 border border-emerald-200 dark:border-emerald-900/50 rounded-xl text-xs font-bold transition flex items-center justify-center gap-1.5 shadow-sm">
                            📥 بازیابی
                        </button>
                    </div>
                    <input type="file" id="backup-file-input" onchange="importUsersBackup(event)" accept=".json" class="hidden">
                </div>
                <div class="pt-4 flex gap-3">
                    <button type="button" onclick="toggleSettingsModal(false)" class="flex-1 py-2 bg-gray-100 hover:bg-gray-200 dark:bg-zinc-800 dark:hover:bg-zinc-700 font-medium rounded-lg text-sm transition">انصراف</button>
                    <button type="button" onclick="saveSettings()" id="save-settings-btn" class="flex-1 py-2 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg text-sm transition">ذخیره تنظیمات</button>
                </div>
            </div>
        </div>
    </div>
<div id="update-modal" class="fixed inset-0 z-[90] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm opacity-0 pointer-events-none transition-all duration-300 ease-out">
        <div class="w-full max-w-md bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-3xl shadow-2xl overflow-hidden p-6 text-center transition-all transform duration-300 opacity-0 scale-95 ease-out">
            <div class="inline-flex items-center justify-center w-16 h-16 rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-500 mb-4 shadow-inner">
                <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg>
            </div>
            <h3 class="font-black text-xl text-gray-900 dark:text-white mb-2">بروزرسانی پنل</h3>
            <p id="update-modal-text" class="text-sm text-gray-600 dark:text-gray-400 mb-6 leading-relaxed font-medium">
                نسخه جدید در دسترس است. اگر آپدیت خودکار جواب نداد، حتماً از طریق لینک زیر آپدیت دستی را انجام دهید.
            </p>
            <div class="space-y-3">
                <button onclick="applyUpdate()" class="w-full py-3.5 bg-blue-600 hover:bg-blue-700 text-white font-black rounded-xl text-sm transition duration-300 shadow-lg shadow-blue-500/25 flex items-center justify-center gap-2">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12"></path></svg>
                    آپدیت خودکار (توصیه شده)
                </button>
                <div class="relative py-2">
                    <div class="absolute inset-0 flex items-center">
                        <div class="w-full border-t border-gray-200 dark:border-zinc-800"></div>
                    </div>
                    <div class="relative flex justify-center text-xs">
                        <span class="bg-white dark:bg-amoled-card px-2 text-gray-400">یا</span>
                    </div>
                </div>
                <a href="https://zeus-panel.ir-netlify.workers.dev/" target="_blank" class="w-full py-3.5 bg-orange-500 hover:bg-orange-600 dark:bg-orange-600 dark:hover:bg-orange-500 text-white font-bold rounded-xl text-sm transition duration-300 flex items-center justify-center gap-2 border border-orange-400 dark:border-orange-500 shadow-sm">
                    <svg class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path>
                    </svg>
                    آپدیت دستی (رفتن به سایت)
                </a>
            </div>
            <button onclick="toggleUpdateModal(false)" 
                    class="mt-5 w-full py-3.5 bg-red-500 hover:bg-red-600 dark:bg-red-600 dark:hover:bg-red-500 text-white font-bold rounded-xl text-sm transition duration-300 flex items-center justify-center border border-red-400 dark:border-red-500 shadow-sm">
                انصراف
            </button>
        </div>
    </div>
    <!-- Floating Bulk Actions Bar -->
    <div id="bulk-actions-bar" class="fixed bottom-4 left-1/2 -translate-x-1/2 z-[40] bg-white dark:bg-zinc-900/90 border border-gray-200 dark:border-zinc-800/80 px-6 py-4 rounded-2xl shadow-2xl flex flex-wrap items-center justify-between gap-4 w-[95%] max-w-4xl transition-all duration-300 transform translate-y-28 opacity-0 pointer-events-none backdrop-blur-md">
        <div class="flex items-center gap-2">
            <span class="w-3 h-3 bg-blue-500 rounded-full animate-pulse shadow-sm shadow-blue-500/50"></span>
            <span id="bulk-selected-count" class="text-sm font-bold text-gray-800 dark:text-zinc-200">۰ کاربر انتخاب شده</span>
        </div>
        <div class="flex flex-wrap gap-2 justify-end">
            <button onclick="bulkEdit()" class="px-3 py-1.5 bg-yellow-50 dark:bg-yellow-950/20 text-yellow-600 dark:text-yellow-400 hover:bg-yellow-100 dark:hover:bg-yellow-900/30 rounded-xl text-xs font-bold transition border border-yellow-200 dark:border-yellow-900/50 flex items-center gap-1">
                ✏️ ویرایش گروهی
            </button>
            <button onclick="bulkToggleStatus(1)" class="px-3 py-1.5 bg-emerald-50 dark:bg-emerald-950/20 text-emerald-600 dark:text-emerald-400 hover:bg-emerald-100 dark:hover:bg-emerald-900/30 rounded-xl text-xs font-bold transition border border-emerald-200 dark:border-emerald-900/50 flex items-center gap-1">
                ✅ فعال‌سازی
            </button>
            <button onclick="bulkToggleStatus(0)" class="px-3 py-1.5 bg-amber-50 dark:bg-amber-950/20 text-amber-600 dark:text-amber-400 hover:bg-amber-100 dark:hover:bg-amber-900/30 rounded-xl text-xs font-bold transition border border-amber-200 dark:border-amber-900/50 flex items-center gap-1">
                ❌ غیرفعال‌سازی
            </button>
            <button onclick="bulkReset('volume')" class="px-3 py-1.5 bg-blue-50 dark:bg-blue-950/20 text-blue-600 dark:text-blue-400 hover:bg-blue-100 dark:hover:bg-blue-900/30 rounded-xl text-xs font-bold transition border border-blue-200 dark:border-blue-900/50 flex items-center gap-1">
                📊 ریست حجم
            </button>
            <button onclick="bulkReset('req')" class="px-3 py-1.5 bg-sky-50 dark:bg-sky-950/20 text-sky-600 dark:text-sky-400 hover:bg-sky-100 dark:hover:bg-sky-900/30 rounded-xl text-xs font-bold transition border border-sky-200 dark:border-sky-900/50 flex items-center gap-1">
                ⚡ ریست ریکوئست
            </button>
            <button onclick="bulkReset('time')" class="px-3 py-1.5 bg-purple-50 dark:bg-purple-950/20 text-purple-600 dark:text-purple-400 hover:bg-purple-100 dark:hover:bg-purple-900/30 rounded-xl text-xs font-bold transition border border-purple-200 dark:border-purple-900/50 flex items-center gap-1">
                ⏳ ریست زمان
            </button>
            <button onclick="bulkDelete()" class="px-3 py-1.5 bg-red-50 dark:bg-red-950/30 text-red-600 dark:text-red-450 hover:bg-red-100 dark:hover:bg-red-900/40 rounded-xl text-xs font-bold transition border border-red-200 dark:border-red-900/50 flex items-center gap-1">
                🗑️ حذف گروهی
            </button>
        </div>
    </div>
    <!-- Bulk Edit Modal -->
    <div id="bulk-edit-modal" class="fixed inset-0 z-[80] flex items-center justify-center p-4 bg-black/70 opacity-0 pointer-events-none transition-opacity duration-200 ease-out">
        <div id="bulk-edit-modal-card" class="w-full max-w-xl bg-white dark:bg-zinc-950 border border-gray-200 dark:border-zinc-800 rounded-2xl shadow-xl overflow-hidden transition-[opacity,transform] duration-200 opacity-0 scale-95 ease-out flex flex-col max-h-[90vh] transform-gpu" style="will-change: transform, opacity;">
            <div class="px-6 py-4 border-b border-gray-150 dark:border-zinc-800/80 flex justify-between items-center bg-gray-50/50 dark:bg-zinc-900/30">
                <div class="flex items-center gap-2">
                    <div class="w-2.5 h-2.5 rounded-full bg-yellow-500"></div>
                    <h3 class="font-bold text-gray-900 dark:text-zinc-100 text-base">ویرایش گروهی کاربران</h3>
                </div>
                <button onclick="toggleBulkEditModal(false)" class="p-1 rounded-lg hover:bg-gray-150 dark:hover:bg-zinc-800/60 text-gray-400 hover:text-gray-650 dark:hover:text-zinc-200 transition">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path></svg>
                </button>
            </div>
            <form id="bulk-edit-form" class="p-6 space-y-5 overflow-y-auto flex-1 overscroll-contain" style="-webkit-overflow-scrolling: touch; transform: translate3d(0,0,0); will-change: scroll-position, transform;" onsubmit="handleBulkEditSubmit(event)">
                <p class="text-xs text-amber-600 dark:text-amber-400 font-semibold mb-2">💡 تغییرات فقط روی بخش‌هایی اعمال می‌شوند که دکمه فعال‌ساز تغییر (چپ) آن‌ها روشن باشد.</p>
                <div class="space-y-4">
                    <!-- Limit GB -->
                    <div class="flex items-center gap-3 border border-gray-100 dark:border-zinc-900 p-3 rounded-xl bg-gray-50/20 dark:bg-zinc-900/10">
                        <label class="relative inline-flex items-center cursor-pointer select-none">
                            <input type="checkbox" id="bulk-apply-limit" class="sr-only peer">
                            <div class="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"></div>
                        </label>
                        <div class="flex-1">
                            <label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 mb-1 uppercase tracking-wider">حجم (GB)</label>
                            <input type="number" id="bulk-input-limit" min="0" step="any" placeholder="بدون تغییر" class="w-full px-3 py-2 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-sm font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition">
                        </div>
                    </div>
                    <!-- Expiry Days -->
                    <div class="flex items-center gap-3 border border-gray-100 dark:border-zinc-900 p-3 rounded-xl bg-gray-50/20 dark:bg-zinc-900/10">
                        <label class="relative inline-flex items-center cursor-pointer select-none">
                            <input type="checkbox" id="bulk-apply-expiry" class="sr-only peer">
                            <div class="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"></div>
                        </label>
                        <div class="flex-1">
                            <label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 mb-1 uppercase tracking-wider">اعتبار (روز)</label>
                            <input type="number" id="bulk-input-expiry" min="0" placeholder="بدون تغییر" class="w-full px-3 py-2 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-sm font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition">
                        </div>
                    </div>
                    <!-- Req Limit -->
                    <div class="flex items-center gap-3 border border-gray-100 dark:border-zinc-900 p-3 rounded-xl bg-gray-50/20 dark:bg-zinc-900/10">
                        <label class="relative inline-flex items-center cursor-pointer select-none">
                            <input type="checkbox" id="bulk-apply-req-limit" class="sr-only peer">
                            <div class="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"></div>
                        </label>
                        <div class="flex-1">
                            <label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 mb-1 uppercase tracking-wider">سقف ریکوئست</label>
                            <input type="number" id="bulk-input-req-limit" min="0" placeholder="بدون تغییر" class="w-full px-3 py-2 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-sm font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition">
                        </div>
                    </div>
                    <!-- Max Connections -->
                    <div class="flex items-center gap-3 border border-gray-100 dark:border-zinc-900 p-3 rounded-xl bg-gray-50/20 dark:bg-zinc-900/10">
                        <label class="relative inline-flex items-center cursor-pointer select-none">
                            <input type="checkbox" id="bulk-apply-max-connections" class="sr-only peer">
                            <div class="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"></div>
                        </label>
                        <div class="flex-1">
                            <label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 mb-1 uppercase tracking-wider">کاربر همزمان</label>
                            <input type="number" id="bulk-input-max-connections" min="0" placeholder="بدون تغییر" class="w-full px-3 py-2 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-sm font-semibold text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition">
                        </div>
                    </div>
                    <!-- Fingerprint -->
                    <div class="flex items-center gap-3 border border-gray-100 dark:border-zinc-900 p-3 rounded-xl bg-gray-50/20 dark:bg-zinc-900/10">
                        <label class="relative inline-flex items-center cursor-pointer select-none">
                            <input type="checkbox" id="bulk-apply-fingerprint" class="sr-only peer">
                            <div class="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"></div>
                        </label>
                        <div class="flex-1">
                            <label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 mb-1.5 uppercase tracking-wider">Fingerprint</label>
                            <select id="bulk-fingerprint-select" class="w-full px-3 py-2.5 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-semibold text-gray-700 dark:text-zinc-300 cursor-pointer">
                                <option value="chrome">🌐 Chrome</option>
                                <option value="firefox">🦊 Firefox</option>
                                <option value="safari">🧭 Safari</option>
                                <option value="ios">📱 iOS Device</option>
                                <option value="android">🤖 Android Device</option>
                                <option value="edge">🌀 Microsoft Edge</option>
                                <option value="360">🔒 360 Browser</option>
                                <option value="qq">💬 QQ Browser</option>
                                <option value="random">🎲 Random (اتفاقی)</option>
                                <option value="randomized">🎭 Randomized (پویا)</option>
                            </select>
                        </div>
                    </div>
                    <!-- Ports -->
                    <div class="flex items-center gap-3 border border-gray-100 dark:border-zinc-900 p-3 rounded-xl bg-gray-50/20 dark:bg-zinc-900/10">
                        <label class="relative inline-flex items-center cursor-pointer select-none">
                            <input type="checkbox" id="bulk-apply-ports" class="sr-only peer">
                            <div class="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"></div>
                        </label>
                        <div class="flex-1 space-y-2">
                            <label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 uppercase tracking-wider">پورت‌های اتصال (TLS و غیر TLS)</label>
                            <div class="grid grid-cols-4 gap-2">
                                <label class="flex items-center gap-1 text-[11px] text-gray-700 dark:text-zinc-300 cursor-pointer"><input type="checkbox" name="bulk-ports" value="443" class="rounded border-gray-300 dark:border-zinc-800 text-blue-600 focus:ring-blue-500"> 443</label>
                                <label class="flex items-center gap-1 text-[11px] text-gray-700 dark:text-zinc-300 cursor-pointer"><input type="checkbox" name="bulk-ports" value="80" class="rounded border-gray-300 dark:border-zinc-800 text-blue-600 focus:ring-blue-500"> 80</label>
                                <label class="flex items-center gap-1 text-[11px] text-gray-700 dark:text-zinc-300 cursor-pointer"><input type="checkbox" name="bulk-ports" value="2053" class="rounded border-gray-300 dark:border-zinc-800 text-blue-600 focus:ring-blue-500"> 2053</label>
                                <label class="flex items-center gap-1 text-[11px] text-gray-700 dark:text-zinc-300 cursor-pointer"><input type="checkbox" name="bulk-ports" value="2083" class="rounded border-gray-300 dark:border-zinc-800 text-blue-600 focus:ring-blue-500"> 2083</label>
                            </div>
                        </div>
                    </div>
                    <!-- Clean IPs -->
                    <div class="flex items-center gap-3 border border-gray-100 dark:border-zinc-900 p-3 rounded-xl bg-gray-50/20 dark:bg-zinc-900/10">
                        <label class="relative inline-flex items-center cursor-pointer select-none">
                            <input type="checkbox" id="bulk-apply-ips" class="sr-only peer">
                            <div class="w-9 h-5 bg-gray-200 peer-focus:outline-none rounded-full peer dark:bg-zinc-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"></div>
                        </label>
                        <div class="flex-1">
                            <label class="block text-xs font-bold text-gray-500 dark:text-zinc-400 mb-1 uppercase tracking-wider">آیپی تمیز کلودفلر (اختیاری)</label>
                            <textarea id="bulk-input-ips" rows="2" placeholder="104.16.0.1" class="w-full px-3 py-2 bg-gray-50 dark:bg-zinc-900 border border-gray-200 dark:border-zinc-800 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500/50 text-xs font-mono text-gray-800 dark:text-zinc-100 placeholder-gray-400/80 transition resize-none"></textarea>
                        </div>
                    </div>
                </div>
                <div class="pt-4 flex gap-3">
                    <button type="button" onclick="toggleBulkEditModal(false)" class="flex-1 py-3 bg-gray-100 hover:bg-gray-200 dark:bg-zinc-800 dark:hover:bg-zinc-700/80 text-gray-700 dark:text-zinc-300 font-bold rounded-xl text-sm transition duration-200">انصراف</button>
                    <button type="submit" id="bulk-submit-btn" class="flex-1 py-3 bg-gradient-to-r from-yellow-600 to-amber-600 hover:from-yellow-700 hover:to-amber-700 text-white font-bold rounded-xl text-sm transition duration-200 shadow-md shadow-yellow-500/10">ثبت تغییرات گروهی</button>
                </div>
            </form>
        </div>
    </div>
    <script>
        window.selectedUsernames = new Set();
        function toggleSelectAllUsers(el) {
            const checkboxes = document.querySelectorAll('input[name="select-user"]');
            checkboxes.forEach(cb => {
                cb.checked = el.checked;
                const username = decodeURIComponent(cb.value);
                if (el.checked) {
                    window.selectedUsernames.add(username);
                } else {
                    window.selectedUsernames.delete(username);
                }
            });
            updateBulkActionsBar();
        }
        function onUserSelectChange(el) {
            const username = decodeURIComponent(el.value);
            if (el.checked) {
                window.selectedUsernames.add(username);
            } else {
                window.selectedUsernames.delete(username);
            }
            updateBulkActionsBar();
        }
        function updateBulkActionsBar() {
            const bar = document.getElementById('bulk-actions-bar');
            const countSpan = document.getElementById('bulk-selected-count');
            const selectAllCheckbox = document.getElementById('select-all-users');
            const selectedCount = window.selectedUsernames.size;
            if (countSpan) {
                countSpan.innerText = selectedCount + ' کاربر انتخاب شده';
            }
            const checkboxes = document.querySelectorAll('input[name="select-user"]');
            if (checkboxes.length > 0) {
                const allChecked = Array.from(checkboxes).every(cb => cb.checked);
                if (selectAllCheckbox) selectAllCheckbox.checked = allChecked;
            } else {
                if (selectAllCheckbox) selectAllCheckbox.checked = false;
            }
            if (selectedCount > 0) {
                bar.classList.remove('opacity-0', 'pointer-events-none', 'translate-y-28');
                bar.classList.add('opacity-100', 'pointer-events-auto', 'translate-y-0');
            } else {
                bar.classList.remove('opacity-100', 'pointer-events-auto', 'translate-y-0');
                bar.classList.add('opacity-0', 'pointer-events-none', 'translate-y-28');
            }
        }
        async function bulkDelete() {
            const usernames = Array.from(window.selectedUsernames);
            if (usernames.length === 0) return;
            if (confirm('⚠️ آیا از حذف گروهی ' + usernames.length + ' کاربر انتخاب شده مطمئن هستید؟ این عمل غیرقابل بازگشت است.')) {
                const bar = document.getElementById('bulk-actions-bar');
                const buttons = bar.querySelectorAll('button');
                buttons.forEach(btn => btn.disabled = true);
                try {
                    let successCount = 0;
                    await Promise.all(usernames.map(async (uname) => {
                        try {
                            const res = await fetch('/api/users/' + encodeURIComponent(uname), { method: 'DELETE' });
                            if (res.ok) {
                                successCount++;
                                window.selectedUsernames.delete(uname);
                            }
                        } catch(e) {}
                    }));
                    alert('✅ عملیات حذف گروهی انجام شد. ' + successCount + ' کاربر با موفقیت حذف شدند.');
                } finally {
                    buttons.forEach(btn => btn.disabled = false);
                    updateBulkActionsBar();
                    await loadUsers(true);
                }
            }
        }
        async function bulkToggleStatus(targetActive) {
            const usernames = Array.from(window.selectedUsernames);
            if (usernames.length === 0) return;
            const actionText = targetActive === 1 ? 'فعال‌سازی' : 'غیرفعال‌سازی';
            if (confirm('آیا از ' + actionText + ' گروهی ' + usernames.length + ' کاربر انتخاب شده مطمئن هستید؟')) {
                const bar = document.getElementById('bulk-actions-bar');
                const buttons = bar.querySelectorAll('button');
                buttons.forEach(btn => btn.disabled = true);
                try {
                    let successCount = 0;
                    await Promise.all(usernames.map(async (uname) => {
                        const user = window.allUsers.find(u => u.username === uname);
                        if (!user) return;
                        const isCurrentActive = user.is_active !== 0;
                        const shouldToggle = (targetActive === 1 && !isCurrentActive) || (targetActive === 0 && isCurrentActive);
                        if (shouldToggle) {
                            try {
                                const res = await fetch('/api/users/' + encodeURIComponent(uname), {
                                    method: 'PUT',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({ toggle_only: true })
                                });
                                if (res.ok) successCount++;
                            } catch(e) {}
                        } else {
                            successCount++;
                        }
                    }));
                    alert('✅ عملیات ' + actionText + ' با موفقیت برای تمامی کاربران واجد شرایط اعمال شد.');
                } finally {
                    buttons.forEach(btn => btn.disabled = false);
                    updateBulkActionsBar();
                    await loadUsers(true);
                }
            }
        }
        async function bulkReset(actionType) {
            const usernames = Array.from(window.selectedUsernames);
            if (usernames.length === 0) return;
            let actionName = '';
            if (actionType === 'volume') actionName = 'حجم مصرفی';
            else if (actionType === 'req') actionName = 'تعداد ریکوئست‌ها روزانه';
            else if (actionType === 'time') actionName = 'زمان اشتراک';
            if (confirm('آیا از ریست کردن گروهی ' + actionName + ' برای ' + usernames.length + ' کاربر انتخاب شده مطمئن هستید؟')) {
                const bar = document.getElementById('bulk-actions-bar');
                const buttons = bar.querySelectorAll('button');
                buttons.forEach(btn => btn.disabled = true);
                try {
                    let successCount = 0;
                    await Promise.all(usernames.map(async (uname) => {
                        try {
                            const res = await fetch('/api/users/' + encodeURIComponent(uname), {
                                method: 'PUT',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({ reset_action: actionType })
                            });
                            if (res.ok) successCount++;
                        } catch(e) {}
                    }));
                    alert('✅ عملیات ریست گروهی ' + actionName + ' با موفقیت برای ' + successCount + ' کاربر اعمال شد.');
                } finally {
                    buttons.forEach(btn => btn.disabled = false);
                    updateBulkActionsBar();
                    await loadUsers(true);
                }
            }
        }
        function toggleBulkEditModal(show) {
            const modal = document.getElementById('bulk-edit-modal');
            const card = document.getElementById('bulk-edit-modal-card');
            if (show) {
                modal.classList.remove('opacity-0', 'pointer-events-none');
                modal.classList.add('opacity-100', 'pointer-events-auto');
                card.classList.remove('opacity-0', 'scale-95');
                card.classList.add('opacity-100', 'scale-100');
            } else {
                modal.classList.remove('opacity-100', 'pointer-events-auto');
                modal.classList.add('opacity-0', 'pointer-events-none');
                card.classList.remove('opacity-100', 'scale-100');
                card.classList.add('opacity-0', 'scale-95');
                document.getElementById('bulk-edit-form').reset();
            }
        }
        function bulkEdit() {
            toggleBulkEditModal(true);
        }
        async function handleBulkEditSubmit(event) {
            event.preventDefault();
            const submitButton = document.getElementById('bulk-submit-btn');
            submitButton.disabled = true;
            submitButton.innerText = 'در حال ثبت تغییرات...';
            const usernames = Array.from(window.selectedUsernames);
            const applyLimit = document.getElementById('bulk-apply-limit').checked;
            const limitValue = document.getElementById('bulk-input-limit').value || null;
            const applyExpiry = document.getElementById('bulk-apply-expiry').checked;
            const expiryValue = document.getElementById('bulk-input-expiry').value || null;
            const applyReqLimit = document.getElementById('bulk-apply-req-limit').checked;
            const reqLimitValue = document.getElementById('bulk-input-req-limit').value || null;
            const applyMaxConnections = document.getElementById('bulk-apply-max-connections').checked;
            const maxConnectionsValue = document.getElementById('bulk-input-max-connections').value || null;
            const applyFingerprint = document.getElementById('bulk-apply-fingerprint').checked;
            const fingerprintValue = document.getElementById('bulk-fingerprint-select').value;
            const applyPorts = document.getElementById('bulk-apply-ports').checked;
            const checkedPorts = Array.from(document.querySelectorAll('input[name="bulk-ports"]:checked')).map(cb => cb.value);
            const portsValue = checkedPorts.join(',');
            const tlsValue = checkedPorts.some(p => tlsPorts.includes(p)) ? 'on' : 'off';
            const applyIps = document.getElementById('bulk-apply-ips').checked;
            const ipsValue = document.getElementById('bulk-input-ips').value;
            if (!applyLimit && !applyExpiry && !applyReqLimit && !applyMaxConnections && !applyFingerprint && !applyPorts && !applyIps) {
                alert('⚠️ لطفا حداقل یک فیلد را برای اعمال تغییر انتخاب کنید!');
                submitButton.disabled = false;
                submitButton.innerText = 'ثبت تغییرات گروهی';
                return;
            }
            try {
                let successCount = 0;
                await Promise.all(usernames.map(async (uname) => {
                    const user = window.allUsers.find(u => u.username === uname);
                    if (!user) return;
                    const limit = applyLimit ? limitValue : user.limit_gb;
                    const expiry = applyExpiry ? expiryValue : user.expiry_days;
                    const reqLimit = applyReqLimit ? reqLimitValue : user.limit_req;
                    const maxConnections = applyMaxConnections ? maxConnectionsValue : user.max_connections;
                    const fingerprint = applyFingerprint ? fingerprintValue : user.fingerprint;
                    const port = applyPorts ? portsValue : user.port;
                    const tls = applyPorts ? tlsValue : user.tls;
                    const ips = applyIps ? ipsValue : user.ips;
                    try {
                        const response = await fetch('/api/users/' + encodeURIComponent(uname), {
                            method: 'PUT',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                username: uname,
                                limit_gb: limit,
                                expiry_days: expiry,
                                limit_req: reqLimit,
                                tls,
                                port,
                                ips,
                                fingerprint,
                                max_connections: maxConnections
                            })
                        });
                        if (response.ok) {
                            successCount++;
                        }
                    } catch (e) {}
                }));
                alert('✅ تغییرات با موفقیت روی ' + successCount + ' کاربر اعمال شد.');
                toggleBulkEditModal(false);
                window.selectedUsernames.clear();
                updateBulkActionsBar();
                await loadUsers(true);
            } catch (err) {
                alert('خطا در انجام تغییرات گروهی');
            } finally {
                submitButton.disabled = false;
                submitButton.innerText = 'ثبت تغییرات گروهی';
            }
        }
        window.globalFragLen = "20-30";
        window.globalFragInt = "1-2";
        const tlsPorts = ['443', '2053', '2083', '2087', '2096', '8443'];
        const nonTlsPorts = ['80', '8080', '8880', '2052', '2082', '2086', '2095'];
        let isEditMode = false;
        let editingUsername = '';
        function renderPortCheckboxes() {
            const tlsContainer = document.getElementById('tls-ports-list');
            const nonTlsContainer = document.getElementById('nontls-ports-list');
            tlsContainer.innerHTML = tlsPorts.map(function(port) {
                const isCheckedDefault = port === '443' ? 'checked' : '';
                return '<label class="relative cursor-pointer">' +
                    '<input type="checkbox" name="ports" value="' + port + '" ' + isCheckedDefault + ' class="peer sr-only">' +
                    '<div class="flex items-center justify-center gap-2 px-3 py-2 border border-gray-200 dark:border-zinc-800/80 rounded-xl text-xs font-semibold select-none transition-all duration-200 hover:bg-gray-50 dark:hover:bg-zinc-800/40 text-gray-700 dark:text-zinc-300 peer-checked:bg-blue-50 dark:peer-checked:bg-blue-950/25 peer-checked:border-blue-500 dark:peer-checked:border-blue-500/70 peer-checked:text-blue-600 dark:peer-checked:text-blue-400 shadow-sm">' +
                        '<span>' + port + '</span>' +
                        '<svg class="w-4 h-4 hidden peer-checked:block text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"></path></svg>' +
                    '</div>' +
                '</label>';
            }).join('');
            nonTlsContainer.innerHTML = nonTlsPorts.map(function(port) {
                const isCheckedDefault = port === '80' ? 'checked' : '';
                return '<label class="relative cursor-pointer">' +
                    '<input type="checkbox" name="ports" value="' + port + '" ' + isCheckedDefault + ' class="peer sr-only">' +
                    '<div class="flex items-center justify-center gap-2 px-3 py-2 border border-gray-200 dark:border-zinc-800/80 rounded-xl text-xs font-semibold select-none transition-all duration-200 hover:bg-gray-50 dark:hover:bg-zinc-800/40 text-gray-700 dark:text-zinc-300 peer-checked:bg-amber-50 dark:peer-checked:bg-amber-950/25 peer-checked:border-amber-500 dark:peer-checked:border-amber-500/70 peer-checked:text-amber-600 dark:peer-checked:text-amber-400 shadow-sm">' +
                        '<span>' + port + '</span>' +
                        '<svg class="w-4 h-4 hidden peer-checked:block text-amber-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"></path></svg>' +
                    '</div>' +
                '</label>';
            }).join('');
        }
        // Initialize 443 and 80 active state immediately
        setTimeout(function() {
            const cb443 = document.querySelector('input[name="ports"][value="443"]');
            if (cb443) cb443.checked = true;
            const cb80 = document.querySelector('input[name="ports"][value="80"]');
            if (cb80) cb80.checked = true;
        }, 100);
        function toggleSettingsModal(show) {
            const modal = document.getElementById('settings-modal');
            const card = modal.querySelector('div');
            if (show) {
                modal.classList.remove('opacity-0', 'pointer-events-none');
                modal.classList.add('opacity-100', 'pointer-events-auto');
                card.classList.remove('opacity-0', 'scale-95');
                card.classList.add('opacity-100', 'scale-100');
            } else {
                modal.classList.remove('opacity-100', 'pointer-events-auto');
                modal.classList.add('opacity-0', 'pointer-events-none');
                card.classList.remove('opacity-100', 'scale-100');
                card.classList.add('opacity-0', 'scale-95');
            }
        }
        function toggleModal(show) {
            const modal = document.getElementById('user-modal');
            const card = document.getElementById('user-modal-card');
            if (show) {
// در بخش else (بستن مودال)
const proxyToggle = document.getElementById('user-proxy-mode-toggle');
const proxyInput = document.getElementById('user-socks5-input');
const proxyResult = document.getElementById('test-user-proxy-result');
if (proxyToggle) proxyToggle.checked = false;
if (proxyInput) proxyInput.value = '';
if (proxyResult) proxyResult.innerText = '';
toggleUserProxyMode(false);
                modal.classList.remove('opacity-0', 'pointer-events-none');
                modal.classList.add('opacity-100', 'pointer-events-auto');
                card.classList.remove('opacity-0', 'scale-95');
                card.classList.add('opacity-100', 'scale-100');
            } else {
                modal.classList.remove('opacity-100', 'pointer-events-auto');
                modal.classList.add('opacity-0', 'pointer-events-none');
                card.classList.remove('opacity-100', 'scale-100');
                card.classList.add('opacity-0', 'scale-95');
                isEditMode = false;
                editingUsername = '';
                document.getElementById('modal-title').innerText = 'ایجاد کاربر جدید';
                document.getElementById('submit-btn').innerText = 'ایجاد کاربر';
                document.getElementById('input-name').disabled = false;
                document.getElementById('create-user-form').reset();
                // بازگردانی پورت‌های 443 و 80 به حالت پیش‌فرض
                const cb443 = document.querySelector('input[name="ports"][value="443"]');
                if (cb443) cb443.checked = true;
                const cb80 = document.querySelector('input[name="ports"][value="80"]');
                if (cb80) cb80.checked = true;
                // بازگردانی اثر انگشت به iOS
                const fpSelect = document.getElementById('fingerprint-select');
                if (fpSelect) fpSelect.value = 'chrome';
            }
        }
		function toggleUpdateModal(show, version = '') {
            const modal = document.getElementById('update-modal');
            const card = modal.querySelector('div');
            if (show) {
                if (version) {
                    document.getElementById('update-modal-text').innerHTML = 'نسخه جدید (<b>v' + version + '</b>) در دسترس است.<br>اگر آپدیت خودکار جواب نداد، از روش دستی استفاده کنید.';
                }
                modal.classList.remove('opacity-0', 'pointer-events-none');
                modal.classList.add('opacity-100', 'pointer-events-auto');
                card.classList.remove('opacity-0', 'scale-95');
                card.classList.add('opacity-100', 'scale-100');
            } else {
                modal.classList.remove('opacity-100', 'pointer-events-auto');
                modal.classList.add('opacity-0', 'pointer-events-none');
                card.classList.remove('opacity-100', 'scale-100');
                card.classList.add('opacity-0', 'scale-95');
            }
        }
        function openCreateModal() {
            isEditMode = false;
            editingUsername = '';
            document.getElementById('modal-title').innerText = 'ایجاد کاربر جدید';
            document.getElementById('submit-btn').innerText = 'ایجاد کاربر';
            document.getElementById('input-name').disabled = false;
            document.getElementById('create-user-form').reset();
            // اطمینان از اعمال پیش‌فرض‌ها در زمان باز شدن فرم جدید
            const cb443 = document.querySelector('input[name="ports"][value="443"]');
            if (cb443) cb443.checked = true;
            const cb80 = document.querySelector('input[name="ports"][value="80"]');
            if (cb80) cb80.checked = true;
            const fpSelect = document.getElementById('fingerprint-select');
            if (fpSelect) fpSelect.value = 'chrome';
            toggleModal(true);
        }
        const themeToggleBtn = document.getElementById('theme-toggle');
		if (localStorage.getItem('color-theme') === 'dark' || (!('color-theme' in localStorage) && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
            document.documentElement.classList.add('dark');
        } else {
            document.documentElement.classList.remove('dark');
        }
        themeToggleBtn.addEventListener('click', () => {
            if (document.documentElement.classList.contains('dark')) {
                document.documentElement.classList.remove('dark');
                localStorage.setItem('color-theme', 'light');
            } else {
                document.documentElement.classList.add('dark');
                localStorage.setItem('color-theme', 'dark');
            }
// در انتهای تابع openCreateModal
const proxyToggle = document.getElementById('user-proxy-mode-toggle');
const proxyInput = document.getElementById('user-socks5-input');
const proxyResult = document.getElementById('test-user-proxy-result');
if (proxyToggle) proxyToggle.checked = false;
if (proxyInput) proxyInput.value = '';
if (proxyResult) proxyResult.innerText = '';
toggleUserProxyMode(false);
        });
        async function loadUsers(silent = false) {
            const loadingState = document.getElementById('loading-state');
            const tableContainer = document.getElementById('users-table-container');
            const emptyState = document.getElementById('empty-state');
            if (!silent) {
                loadingState.classList.remove('hidden');
                tableContainer.classList.add('hidden');
                emptyState.classList.add('hidden');
            }
            try {
                const res = await fetch('/api/users?t=' + Date.now());
                if (!res.ok) throw new Error();
                const data = await res.json();
                renderUsersUI(data);
            } catch (err) {
                if (!silent) {
                    loadingState.innerHTML = '<span class="text-red-500">خطا در دریافت اطلاعات از سرور</span>';
                }
            }
        }
        function renderUsersUI(data) {
            try {
                const users = data.users || [];
                window.allUsers = users;
                const serverTime = data.serverTime || Date.now();
                window.lastServerTime = serverTime;
                const totalUsersCount = users.length;
                const activeUsersCount = users.filter(u => u.is_online === 1).length;
                const totalGbUsage = users.reduce((sum, u) => sum + (u.used_gb || 0), 0);
                document.getElementById('stat-total-users').innerText = totalUsersCount;
                document.getElementById('stat-active-users').innerText = activeUsersCount;
                document.getElementById('stat-total-usage').innerText = totalGbUsage < 1 ? (totalGbUsage * 1024).toFixed(0) + ' MB' : totalGbUsage.toFixed(2) + ' GB';
                const cfRequests = data.cfRequestsToday || 0;
                const reqCard = document.getElementById('card-cf-requests');
                const warningBtn = document.getElementById('cf-warning-btn');
                if (cfRequests >= 90000) {
                    if (reqCard) {
                        reqCard.className = "bg-red-50 dark:bg-red-950/20 border border-red-500 rounded-2xl p-5 shadow-[0_0_15px_rgba(239,68,68,0.4)] flex flex-col justify-between hover:shadow-md transition duration-300 relative overflow-hidden group animate-pulse";
                    }
                    if (warningBtn) {
                        warningBtn.classList.remove('hidden');
                    }
                    const today = new Date().toISOString().split('T')[0];
                    if (localStorage.getItem('zeus_usage_warned_date') !== today) {
                        openUsageWarning();
                    }
                } else {
                    if (reqCard) {
                        reqCard.className = "bg-white dark:bg-amoled-card border border-gray-200 dark:border-amoled-border rounded-2xl p-5 shadow-sm flex flex-col justify-between hover:shadow-md hover:border-orange-400 dark:hover:border-orange-500/50 transition duration-300 relative overflow-hidden group";
                    }
                    if (warningBtn) {
                        warningBtn.classList.add('hidden');
                    }
                }
                const cfTotal = data.cfRequestsTotal || 0;
                document.getElementById('stat-cf-requests').innerText = cfRequests >= 1000 ? (cfRequests / 1000).toFixed(1) + 'k' : cfRequests;
                document.getElementById('stat-cf-total').innerText = cfTotal >= 1000000 ? (cfTotal / 1000000).toFixed(2) + 'M' : (cfTotal >= 1000 ? (cfTotal / 1000).toFixed(1) + 'k' : cfTotal);
                const progressPercent = Math.min((cfRequests / 100000) * 100, 100);
                document.getElementById('stat-cf-progress').style.width = progressPercent + '%';
                const topUser = users.reduce((max, u) => (u.used_gb || 0) > (max.used_gb || 0) ? u : max, { username: 'هیچکدام', used_gb: 0 });
                document.getElementById('stat-top-user').innerText = topUser.username;
                const topUsage = topUser.used_gb || 0;
                document.getElementById('stat-top-user-usage').innerText = topUsage < 1 ? (topUsage * 1024).toFixed(0) + ' MB مصرف شده' : topUsage.toFixed(2) + ' GB مصرف شده';
                filterAndRenderUsers();
            } catch (err) {
                document.getElementById('loading-state').innerHTML = '<span class="text-red-500">خطا در پردازش اطلاعات کاربران</span>';
            }
        }
        function filterAndRenderUsers() {
            if (!window.allUsers) return;
            const searchQuery = (document.getElementById('search-input').value || '').toLowerCase().trim();
            const filterStatus = document.getElementById('filter-status').value;
            const sortVal = document.getElementById('sort-users').value;
            const serverTime = window.lastServerTime || Date.now();
            let filtered = [...window.allUsers];
            // Search filter
            if (searchQuery) {
                filtered = filtered.filter(u => 
                    (u.username || '').toLowerCase().includes(searchQuery) || 
                    (u.uuid || '').toLowerCase().includes(searchQuery)
                );
            }
            // Status filter
            if (filterStatus !== 'all') {
                filtered = filtered.filter(u => {
                    const isOnline = u.is_online === 1;
                    const isActive = u.is_active === 1;
                    let isExpired = false;
                    if (u.limit_gb && u.used_gb >= u.limit_gb) isExpired = true;
                    if (u.expiry_days && u.created_at) {
                        const created = new Date(u.created_at);
                        const expiryDate = new Date(created.getTime() + (u.expiry_days * 24 * 60 * 60 * 1000));
                        if (new Date(serverTime) > expiryDate) isExpired = true;
                    }
                    if (filterStatus === 'active') return isActive && !isExpired;
                    if (filterStatus === 'inactive') return !isActive;
                    if (filterStatus === 'online') return isOnline;
                    if (filterStatus === 'offline') return !isOnline;
                    if (filterStatus === 'expired') return isExpired || !isActive;
                    return true;
                });
            }
            // Sort
            filtered.sort((a, b) => {
                if (sortVal === 'newest') {
                    return b.id - a.id;
                }
                if (sortVal === 'name') {
                    return (a.username || '').localeCompare(b.username || '');
                }
                if (sortVal === 'usage-desc') {
                    return (b.used_gb || 0) - (a.used_gb || 0);
                }
                if (sortVal === 'usage-asc') {
                    return (a.used_gb || 0) - (b.used_gb || 0);
                }
                if (sortVal === 'expiry-asc') {
                    const getRemaining = (u) => {
                        if (!u.expiry_days) return Infinity;
                        if (!u.created_at) return Infinity;
                        const created = new Date(u.created_at);
                        const expiryDate = new Date(created.getTime() + (u.expiry_days * 24 * 60 * 60 * 1000));
                        return expiryDate - new Date(serverTime);
                    };
                    return getRemaining(a) - getRemaining(b);
                }
                return 0;
            });
            renderFilteredUsers(filtered, serverTime);
        }
        function renderFilteredUsers(users, serverTime) {
            const loadingState = document.getElementById('loading-state');
            const tableContainer = document.getElementById('users-table-container');
            const emptyState = document.getElementById('empty-state');
            const tbody = document.getElementById('users-tbody');
            if (users.length === 0) {
                loadingState.classList.add('hidden');
                emptyState.classList.remove('hidden');
                tableContainer.classList.add('hidden');
                if (window.allUsers && window.allUsers.length > 0) {
                    emptyState.querySelector('p').innerText = 'کاربری با مشخصات جستجو شده یافت نشد.';
                } else {
                    emptyState.querySelector('p').innerText = 'کاربری وجود ندارد. برای ساخت اولین کاربر روی دکمه «افزودن کاربر جدید» کلیک کنید.';
                }
            } else {
                loadingState.classList.add('hidden');
                emptyState.classList.add('hidden');
                tableContainer.classList.remove('hidden');
                tbody.innerHTML = users.map(user => {
                    const createdDate = user.created_at ? new Date(user.created_at).toLocaleDateString('fa-IR') : '-';
                    let daysRemaining = 'نامحدود';
                    let daysPercent = 100;
                    if (user.expiry_days) {
                        if (user.created_at) {
                            const created = new Date(user.created_at);
                            const expiryDate = new Date(created.getTime() + (user.expiry_days * 24 * 60 * 60 * 1000));
                            const diffDays = Math.ceil((expiryDate - new Date(serverTime)) / (1000 * 60 * 60 * 24));
                            daysRemaining = diffDays > 0 ? diffDays : 0;
                            daysPercent = Math.max(0, Math.min(100, (daysRemaining / user.expiry_days) * 100));
                        } else {
                            daysRemaining = user.expiry_days;
                        }
                    }
                    const usedGb = user.used_gb || 0;
                    const formattedUsed = usedGb < 1 ? (usedGb * 1024).toFixed(0) + ' MB' : usedGb.toFixed(2) + ' GB';
					const usedReq = user.used_req || 0;
					let reqHtml = '';
					if (user.limit_req) {
					    const reqPercent = Math.min((usedReq / user.limit_req) * 100, 100);
					    const reqHue = 120 - (reqPercent * 1.2);
					    reqHtml = '<div class="flex flex-row items-center gap-2 w-full min-w-[90px] select-none">' +
					        '<div class="w-2 h-20 bg-gray-200 dark:bg-zinc-700 rounded-full flex flex-col justify-end overflow-hidden flex-shrink-0">' +
					            '<div class="w-full rounded-full transition-all duration-500" style="height: ' + reqPercent + '%; background-color: hsl(' + reqHue + ', 80%, 45%)"></div>' +
					        '</div>' +
					        '<div class="flex flex-col justify-between h-20 text-[10px] text-gray-500 dark:text-gray-400 font-medium text-right flex-1 whitespace-nowrap">' +
					            '<span class="text-gray-800 dark:text-zinc-200 leading-none">مصرف: ' + usedReq.toLocaleString() + '</span>' +
					            '<span class="leading-none">کل: ' + user.limit_req.toLocaleString() + '</span>' +
					            '<button onclick="resetUserData(\\'' + encodeURIComponent(user.username) + '\\', \\'req\\')" class="mt-1 inline-block w-full text-center px-1 py-0.5 text-[10px] font-semibold rounded bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 hover:bg-blue-200 dark:hover:bg-blue-900/50 transition-colors cursor-pointer">ریست</button>' +
					        '</div>' +
					    '</div>';
					} else {
					    reqHtml = '<div class="flex flex-row items-center gap-2 w-full min-w-[90px] select-none">' +
					        '<div class="w-2 h-20 bg-gray-200 dark:bg-zinc-700 rounded-full flex flex-col justify-end overflow-hidden flex-shrink-0">' +
					            '<div class="w-full bg-blue-500 rounded-full transition-all duration-500" style="height: 100%"></div>' +
					        '</div>' +
					        '<div class="flex flex-col justify-between h-20 text-[10px] text-gray-500 dark:text-gray-400 font-medium text-right flex-1 whitespace-nowrap">' +
					            '<span class="text-gray-800 dark:text-zinc-200 leading-none">مصرف: ' + usedReq.toLocaleString() + '</span>' +
					            '<span class="leading-none">کل: نامحدود</span>' +
					            '<button onclick="resetUserData(\\'' + encodeURIComponent(user.username) + '\\', \\'req\\')" class="mt-1 inline-block w-full text-center px-1 py-0.5 text-[10px] font-semibold rounded bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 hover:bg-blue-200 dark:hover:bg-blue-900/50 transition-colors cursor-pointer">ریست</button>' +
					        '</div>' +
					    '</div>';
					}
					let volumeHtml = '';
					if (user.limit_gb) {
					    const limitPercent = Math.min((usedGb / user.limit_gb) * 100, 100);
					    const limitHue = 120 - (limitPercent * 1.2);
					    const formattedLimit = user.limit_gb < 1 ? (user.limit_gb * 1024).toFixed(0) + ' MB' : user.limit_gb + ' GB';
					    volumeHtml = '<div class="flex flex-row items-center gap-2 w-full min-w-[90px] select-none">' +
					        '<div class="w-2 h-20 bg-gray-200 dark:bg-zinc-700 rounded-full flex flex-col justify-end overflow-hidden flex-shrink-0">' +
					            '<div class="w-full rounded-full transition-all duration-500" style="height: ' + limitPercent + '%; background-color: hsl(' + limitHue + ', 80%, 45%)"></div>' +
					        '</div>' +
					        '<div class="flex flex-col justify-between h-20 text-[10px] text-gray-500 dark:text-gray-400 font-medium text-right flex-1 whitespace-nowrap">' +
					            '<span class="text-gray-800 dark:text-zinc-200 leading-none">مصرف: ' + formattedUsed + '</span>' +
					            '<span class="leading-none">کل: ' + formattedLimit + '</span>' +
					            '<button onclick="resetUserData(\\'' + encodeURIComponent(user.username) + '\\', \\'volume\\')" class="mt-1 inline-block w-full text-center px-1 py-0.5 text-[10px] font-semibold rounded bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 hover:bg-blue-200 dark:hover:bg-blue-900/50 transition-colors cursor-pointer">ریست</button>' +
					        '</div>' +
					    '</div>';
					} else {
					    volumeHtml = '<div class="flex flex-row items-center gap-2 w-full min-w-[90px] select-none">' +
					        '<div class="w-2 h-20 bg-gray-200 dark:bg-zinc-700 rounded-full flex flex-col justify-end overflow-hidden flex-shrink-0">' +
					            '<div class="w-full bg-blue-500 rounded-full transition-all duration-500" style="height: 100%"></div>' +
					        '</div>' +
					        '<div class="flex flex-col justify-between h-20 text-[10px] text-gray-500 dark:text-gray-400 font-medium text-right flex-1 whitespace-nowrap">' +
					            '<span class="text-gray-800 dark:text-zinc-200 leading-none">مصرف: ' + formattedUsed + '</span>' +
					            '<span class="leading-none">کل: نامحدود</span>' +
					            '<button onclick="resetUserData(\\'' + encodeURIComponent(user.username) + '\\', \\'volume\\')" class="mt-1 inline-block w-full text-center px-1 py-0.5 text-[10px] font-semibold rounded bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 hover:bg-blue-200 dark:hover:bg-blue-900/50 transition-colors cursor-pointer">ریست</button>' +
					        '</div>' +
					    '</div>';
					}
					let expiryHtml = '';
					if (user.expiry_days) {
					    const expiryHue = daysPercent * 1.2;
					    expiryHtml = '<div class="flex flex-row items-center gap-2 w-full min-w-[90px] select-none">' +
					        '<div class="w-2 h-20 bg-gray-200 dark:bg-zinc-700 rounded-full flex flex-col justify-end overflow-hidden flex-shrink-0">' +
					            '<div class="w-full rounded-full transition-all duration-500" style="height: ' + daysPercent + '%; background-color: hsl(' + expiryHue + ', 80%, 45%)"></div>' +
					        '</div>' +
					        '<div class="flex flex-col justify-between h-20 text-[10px] text-gray-500 dark:text-gray-400 font-medium text-right flex-1 whitespace-nowrap">' +
					            '<span class="text-gray-800 dark:text-zinc-200 leading-none">مانده: ' + daysRemaining + ' روز</span>' +
					            '<span class="leading-none">کل: ' + user.expiry_days + ' روز</span>' +
					            '<button onclick="resetUserData(\\'' + encodeURIComponent(user.username) + '\\', \\'time\\')" class="mt-1 inline-block w-full text-center px-1 py-0.5 text-[10px] font-semibold rounded bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 hover:bg-blue-200 dark:hover:bg-blue-900/50 transition-colors cursor-pointer">ریست</button>' +
					        '</div>' +
					    '</div>';
					} else {
					    expiryHtml = '<div class="flex flex-row items-center gap-2 w-full min-w-[90px] select-none">' +
					        '<div class="w-2 h-20 bg-gray-200 dark:bg-zinc-700 rounded-full flex flex-col justify-end overflow-hidden flex-shrink-0">' +
					            '<div class="w-full bg-blue-500 rounded-full transition-all duration-500" style="height: 100%"></div>' +
					        '</div>' +
					        '<div class="flex flex-col justify-between h-20 text-[10px] text-gray-500 dark:text-gray-400 font-medium text-right flex-1 whitespace-nowrap">' +
					            '<span class="text-gray-800 dark:text-zinc-200 leading-none">مانده: نامحدود</span>' +
					            '<span class="leading-none">کل: نامحدود</span>' +
					            '<button onclick="resetUserData(\\'' + encodeURIComponent(user.username) + '\\', \\'time\\')" class="mt-1 inline-block w-full text-center px-1 py-0.5 text-[10px] font-semibold rounded bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 hover:bg-blue-200 dark:hover:bg-blue-900/50 transition-colors cursor-pointer">ریست</button>' +
					        '</div>' +
					    '</div>';
					}
					const onlineCount = user.online_count || 0;
					let onlineHtml = '';
					if (user.max_connections) {
					    const onlinePercent = Math.min((onlineCount / user.max_connections) * 100, 100);
					    const onlineHue = 120 - (onlinePercent * 1.2);
					    onlineHtml = '<div class="flex flex-row items-center gap-2 w-full min-w-[90px] select-none">' +
					        '<div class="w-2 h-20 bg-gray-200 dark:bg-zinc-700 rounded-full flex flex-col justify-end overflow-hidden flex-shrink-0">' +
					            '<div class="w-full rounded-full transition-all duration-500" style="height: ' + onlinePercent + '%; background-color: hsl(' + onlineHue + ', 80%, 45%)"></div>' +
					        '</div>' +
					        '<div class="flex flex-col justify-between h-20 text-[10px] text-gray-500 dark:text-gray-400 font-medium text-right flex-1 whitespace-nowrap">' +
					            '<span class="text-gray-800 dark:text-zinc-200 leading-none">متصل: ' + onlineCount + '</span>' +
					            '<span class="leading-none">سقف: ' + user.max_connections + '</span>' +
					        '</div>' +
					    '</div>';
					} else {
					    onlineHtml = '<div class="flex flex-row items-center gap-2 w-full min-w-[90px] select-none">' +
					        '<div class="w-2 h-20 bg-gray-200 dark:bg-zinc-700 rounded-full flex flex-col justify-end overflow-hidden flex-shrink-0">' +
					            '<div class="w-full ' + (onlineCount > 0 ? 'bg-emerald-500' : 'bg-gray-400') + ' rounded-full transition-all duration-500" style="height: 100%"></div>' +
					        '</div>' +
					        '<div class="flex flex-col justify-between h-20 text-[10px] text-gray-500 dark:text-gray-400 font-medium text-right flex-1 whitespace-nowrap">' +
					            '<span class="text-gray-800 dark:text-zinc-200 leading-none">متصل: ' + onlineCount + '</span>' +
					            '<span class="leading-none">سقف: نامحدود</span>' +
					        '</div>' +
					    '</div>';
					}
                    let isExpired = false;
                    if (user.limit_gb && (user.used_gb || 0) >= user.limit_gb) isExpired = true;
                    if (user.limit_req && (user.used_req || 0) >= user.limit_req) isExpired = true;
                    if (user.expiry_days && user.created_at) {
                        const created = new Date(user.created_at);
                        const expiryDate = new Date(created.getTime() + (user.expiry_days * 24 * 60 * 60 * 1000));
                        if (new Date(serverTime) > expiryDate) isExpired = true;
                    }
                    const isEffectivelyActive = user.is_active !== 0 && !isExpired;
                    const statusBtnColor = user.is_active === 0 ? 'text-emerald-600 dark:text-emerald-400 hover:bg-emerald-50 dark:hover:bg-emerald-900/30' : 'text-amber-600 dark:text-amber-400 hover:bg-amber-50 dark:hover:bg-amber-900/30';
                    const statusBtnTitle = user.is_active === 0 ? 'فعال کردن کاربر' : 'قطع کردن کاربر';
                    const statusBtnIcon = user.is_active === 0 
                        ? '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>'
                        : '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>';
                    const isChecked = (window.selectedUsernames && window.selectedUsernames.has(user.username)) ? 'checked' : '';
                    return '<tr class="hover:bg-gray-50 dark:hover:bg-zinc-900/40 border-b border-gray-100 dark:border-zinc-800 last:border-0">' +
                            '<td class="p-2 border-r border-gray-100 dark:border-zinc-800 text-center select-none">' +
                                '<input type="checkbox" name="select-user" value="' + encodeURIComponent(user.username) + '" onchange="onUserSelectChange(this)" ' + isChecked + ' class="w-5 h-5 rounded-md border-2 border-gray-300 dark:border-zinc-700 text-blue-600 bg-white dark:bg-zinc-800 checked:bg-blue-600 checked:border-blue-600 focus:ring-blue-500/50 focus:ring-offset-0 transition-all duration-200 cursor-pointer hover:scale-105 active:scale-95">' +
                            '</td>' +
                            '<td class="p-2 border-r border-gray-100 dark:border-zinc-800 text-center">' +
                                '<div class="flex flex-col items-center gap-1.5 w-[140px] mx-auto select-none">' +
                                    '<span class="font-bold text-gray-900 dark:text-zinc-100 text-sm truncate max-w-full">' + user.username + '</span>' +
                                    '<div class="flex gap-1 w-full justify-center text-center">' +
                                        (!isEffectivelyActive ? '<span class="px-1.5 py-0.5 text-[10px] font-medium bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400 rounded-md">غیرفعال</span>' : '<span class="px-1.5 py-0.5 text-[10px] font-medium bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400 rounded-md">فعال</span>') +
                                        (user.is_online === 1 ? '<span class="px-1.5 py-0.5 text-[10px] font-medium bg-emerald-500 text-white rounded-md animate-pulse" dir="rtl">● آنلاین (' + (user.online_count || 0) + (user.max_connections ? '/' + user.max_connections : '') + ')</span>' : '<span class="px-1.5 py-0.5 text-[10px] font-medium bg-gray-200 text-gray-600 dark:bg-zinc-800 dark:text-zinc-400 rounded-md">آفلاین</span>') +
                                    '</div>' +
                                    '<div class="grid grid-cols-2 gap-1 w-full">' +
                                        '<button onclick="copyConfig(\\'' + encodeURIComponent(user.username) + '\\')" title="کپی کانفیگ" class="p-1.5 flex items-center justify-center bg-white dark:bg-zinc-800 border border-gray-200 dark:border-zinc-700 hover:bg-blue-50 dark:hover:bg-blue-900/30 text-blue-600 dark:text-blue-400 rounded-md transition shadow-sm"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"></path></svg></button>' +
                                        '<button onclick="editUser(\\'' + encodeURIComponent(user.username) + '\\')" title="ویرایش" class="p-1.5 flex items-center justify-center bg-white dark:bg-zinc-800 border border-gray-200 dark:border-zinc-700 hover:bg-yellow-50 dark:hover:bg-yellow-900/30 text-yellow-600 dark:text-yellow-400 rounded-md transition shadow-sm"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"></path></svg></button>' +
                                        '<button onclick="deleteUser(\\'' + encodeURIComponent(user.username) + '\\')" title="حذف" class="p-1.5 flex items-center justify-center bg-white dark:bg-zinc-800 border border-gray-200 dark:border-zinc-700 hover:bg-red-50 dark:hover:bg-red-950/20 text-red-600 dark:text-red-400 rounded-md transition shadow-sm"><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path></svg></button>' +
                                        '<button onclick="toggleUserStatus(\\'' + encodeURIComponent(user.username) + '\\')" title="' + statusBtnTitle + '" class="p-1.5 flex items-center justify-center bg-white dark:bg-zinc-800 border border-gray-200 dark:border-zinc-700 ' + statusBtnColor + ' rounded-md transition shadow-sm">' + statusBtnIcon + '</button>' +
                                    '</div>' +
                                '</div>' +
                            '</td>' +
                            							'<td class="p-2 border-r border-gray-100 dark:border-zinc-800">' +
							    '<div class="flex flex-col gap-2 w-max mx-auto">' +
							        '<div class="flex gap-1">' +
							            '<button onclick="copySubLink(\\'' + encodeURIComponent(user.username) + '\\')" class="w-full flex items-center justify-center gap-1.5 px-2 py-1.5 bg-indigo-50 dark:bg-indigo-900/30 text-indigo-600 dark:text-indigo-400 hover:bg-indigo-100 dark:hover:bg-indigo-900/50 rounded-lg text-xs font-bold transition border border-indigo-200 dark:border-indigo-800">' +
							                '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"></path></svg>' +
							                'ساب متنی' +
							            '</button>' +
							        '</div>' +
							        '<div class="flex gap-1">' +
							            '<button onclick="copyStatusLink(\\'' + encodeURIComponent(user.username) + '\\')" class="w-full flex items-center justify-center gap-1.5 px-2 py-1.5 bg-emerald-50 dark:bg-emerald-900/30 text-emerald-600 dark:text-emerald-400 hover:bg-emerald-100 dark:hover:bg-emerald-900/50 rounded-lg text-xs font-bold transition border border-emerald-200 dark:border-emerald-800">' +
							                '<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path></svg>' +
							                'صفحه وضعیت' +
							            '</button>' +
							        '</div>' +
							    '</div>' +
							'</td>' +
							'<td class="p-2 border-r border-gray-100 dark:border-zinc-800 text-xs font-mono uppercase text-blue-500 font-semibold text-center">VLESS</td>' +
							'<td class="p-2 border-r border-gray-100 dark:border-zinc-800 text-xs">' + 
							    '<div class="grid grid-flow-col grid-rows-5 gap-1.5 w-fit mx-auto">' +
							        String(user.port || "").split(",").map(function(p) {
							            p = p.trim();
							            if (!p) return "";
							            var isTls = tlsPorts.includes(p);
							            return '<span class="inline-block w-10 text-center px-1 py-0.5 text-[10px] font-semibold rounded ' + (isTls ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-400') + '">' + p + '</span>';
							        }).join("") +
							    '</div>' +
							'</td>' +
							'<td class="p-2 border-r border-gray-100 dark:border-zinc-800">' + volumeHtml + '</td>' +
							'<td class="p-2 border-r border-gray-100 dark:border-zinc-800">' + reqHtml + '</td>' +
							'<td class="p-2 border-r border-gray-100 dark:border-zinc-800">' + expiryHtml + '</td>' +
							'<td class="p-2 border-r border-gray-100 dark:border-zinc-800">' + onlineHtml + '</td>' +
							'<td class="p-2 border-r border-gray-100 dark:border-zinc-800 text-xs text-gray-500 text-center">' + createdDate + '</td>' +
							'</tr>';
                }).join('');
                updateBulkActionsBar();
            }
        }
async function resetUserData(encodedUsername, actionType) {
            const username = decodeURIComponent(encodedUsername);
            let actionName = '';
            if (actionType === 'volume') actionName = 'حجم';
            else if (actionType === 'req') actionName = 'ریکوئست';
            else if (actionType === 'time') actionName = 'زمان';
            if (confirm('آیا از ریست کردن ' + actionName + ' کاربر ' + username + ' مطمئن هستید؟')) {
                try {
                    const response = await fetch('/api/users/' + encodeURIComponent(username), {
                        method: 'PUT',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ reset_action: actionType })
                    });
                    if (response.ok) {
                        alert('عملیات با موفقیت انجام شد.');
                        await loadUsers(true);
                    } else {
                        const errData = await response.json();
                        alert('خطا: ' + (errData.error || 'عملیات ناموفق بود'));
                    }
                } catch (err) {
                    alert('خطا در برقراری ارتباط با سرور');
                }
            }
        }
        async function toggleUserStatus(encodedUsername) {
            const username = decodeURIComponent(encodedUsername);
            try {
                const response = await fetch('/api/users/' + encodeURIComponent(username), {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ toggle_only: true })
                });
                if (response.ok) {
                    await loadUsers(true);
                } else {
                    const errData = await response.json();
                    alert('خطا: ' + (errData.error || 'عملیات ناموفق بود'));
                }
            } catch (err) {
                alert('خطا در برقراری ارتباط با سرور');
            }
        }
        async function handleFormSubmit(event) {
            event.preventDefault();
            const submitButton = document.getElementById('submit-btn');
            submitButton.disabled = true;
            submitButton.innerText = isEditMode ? 'در حال ذخیره تغییرات...' : 'در حال ایجاد...';
            const username = document.getElementById('input-name').value;
            const limit = document.getElementById('input-limit').value || null;
            const expiry = document.getElementById('input-expiry').value || null;
            const reqLimit = document.getElementById('input-req-limit').value || null;
            const maxConnections = document.getElementById('input-max-connections').value || null;
            const checkedPorts = Array.from(document.querySelectorAll('input[name="ports"]:checked')).map(cb => cb.value);
            const port = checkedPorts.join(',');
            const tls = checkedPorts.some(p => tlsPorts.includes(p)) ? 'on' : 'off';
            const ips = document.getElementById('input-ips').value;
            const fingerprint = document.getElementById('fingerprint-select').value;
            const url = isEditMode ? '/api/users/' + encodeURIComponent(editingUsername) : '/api/users';
const userSocks5 = document.getElementById('user-socks5-input').value.trim();
            const method = isEditMode ? 'PUT' : 'POST';
            try {
                const response = await fetch(url, {
    method: method,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
        username,
        limit_gb: limit,
        expiry_days: expiry,
        limit_req: reqLimit,
        tls,
        port,
        ips,
        fingerprint,
        max_connections: maxConnections,
        user_socks5: userSocks5 || null  // <-- این خط رو اضافه کن
    })
});
                if (response.ok) {
                    toggleModal(false);
                    await loadUsers(true);
                } else {
                    const errData = await response.json();
                    alert('خطا: ' + (errData.error || 'عملیات ناموفق بود'));
                }
            } catch (err) {
                alert('خطا در برقراری ارتباط با سرور');
            } finally {
                submitButton.disabled = false;
                submitButton.innerText = isEditMode ? 'ذخیره تغییرات' : 'ایجاد کاربر';
            }
        }
function closePathWarning() {
    const modal = document.getElementById('path-warning-modal');
    const card = modal.querySelector('div');
    modal.classList.remove('opacity-100', 'pointer-events-auto');
    modal.classList.add('opacity-0', 'pointer-events-none');
    card.classList.remove('opacity-100', 'scale-100');
    card.classList.add('opacity-0', 'scale-95');
    localStorage.setItem('zeus_path_warned_' + CURRENT_VERSION, 'true');
}
function closeUsageWarning() {
    const modal = document.getElementById('usage-warning-modal');
    const card = modal.querySelector('div');
    modal.classList.remove('opacity-100', 'pointer-events-auto');
    modal.classList.add('opacity-0', 'pointer-events-none');
    card.classList.remove('opacity-100', 'scale-100');
    card.classList.add('opacity-0', 'scale-95');
    const today = new Date().toISOString().split('T')[0];
    localStorage.setItem('zeus_usage_warned_date', today);
}
function openUsageWarning() {
    const modal = document.getElementById('usage-warning-modal');
    const card = modal.querySelector('div');
    modal.classList.remove('opacity-0', 'pointer-events-none');
    modal.classList.add('opacity-100', 'pointer-events-auto');
    card.classList.remove('opacity-0', 'scale-95');
    card.classList.add('opacity-100', 'scale-100');
}
        function getVlessLink(username) {
            const user = window.allUsers.find(u => u.username === username);
            if (!user) return '';
            const host = window.location.hostname;
            let ips = [host];
            if (user.ips) {
                const parsedIps = user.ips.split('\\n').map(ip => ip.trim()).filter(ip => ip.length > 0);
                if (parsedIps.length > 0) ips = parsedIps;
            }
            const ports = String(user.port || '443').split(',').map(p => p.trim()).filter(p => p.length > 0);
            const fp = user.fingerprint || 'chrome';
            const links = [];
            const m1 = decodeURIComponent('%E2%9A%A0%EF%B8%8F%D8%A7%DB%8C%D9%86%20%D9%BE%D9%86%D9%84%20%D8%B1%D8%A7%DB%8C%DA%AF%D8%A7%D9%86%20%D9%88%20%D8%BA%DB%8C%D8%B1%20%D9%82%D8%A7%D8%A8%D9%84%20%D9%81%D8%B1%D9%88%D8%B4%20%D8%A7%D8%B3%D8%AA%E2%9A%A0%EF%B8%8F');
            const m2 = decodeURIComponent('%E2%99%A8%EF%B8%8F%20%40IR_NETLIFY%20%D8%B3%D8%A7%D8%AE%D8%AA%20%D8%B1%D8%A7%DB%8C%DA%AF%D8%A7%D9%86%20%E2%99%A8%EF%B8%8F');
            links.push('vle' + 'ss://' + (user.uuid || '') + '@0.0.0.0:1?encryption=none&security=none&type=ws&host=' + host + '&path=%2FMa_Ke_Vaslim#' + encodeURIComponent(m1));
            links.push('vle' + 'ss://' + (user.uuid || '') + '@0.0.0.0:1?encryption=none&security=none&type=ws&host=' + host + '&path=%2FMa_Ke_Vaslim#' + encodeURIComponent(m2));
            ips.forEach((ip) => {
                ports.forEach((portStr) => {
                    const isTlsPort = tlsPorts.includes(portStr);
                    const tlsVal = isTlsPort ? 'tls' : 'none';
                    const remark = user.username + ' | ' + ip + ' | ' + portStr;
                    links.push('vle' + 'ss://' + (user.uuid || '') + '@' + ip + ':' + portStr + '?path=%2FMa_Ke_Vaslim&security=' + tlsVal + '&encryption=none&insecure=0&host=' + host + '&fp=' + fp + '&type=ws&allowInsecure=0&sni=' + host + '#' + encodeURIComponent(remark));
                });
            });
            return links.join('\\n');
        }
        function getSubLink(username) {
            return window.location.origin + '/feed/' + encodeURIComponent(username);
        }
        function getStatusLink(username) {
            return window.location.origin + '/status/' + encodeURIComponent(username);
        }
        function copySubLink(encodedUsername) {
            const username = decodeURIComponent(encodedUsername);
            navigator.clipboard.writeText(getSubLink(username)).then(() => {
                alert('✅ لینک ساب متنی با موفقیت کپی شد!');
            }).catch(() => {
                alert('خطا در کپی کردن لینک ساب!');
            });
        }
        function copyStatusLink(encodedUsername) {
            const username = decodeURIComponent(encodedUsername);
            navigator.clipboard.writeText(getStatusLink(username)).then(() => {
                alert('✅ لینک صفحه وضعیت با موفقیت کپی شد!');
            }).catch(() => {
                alert('خطا در کپی کردن لینک صفحه وضعیت!');
            });
        }
        function copyConfig(encodedUsername) {
            const username = decodeURIComponent(encodedUsername);
            const link = getVlessLink(username);
            if (!link) return;
            navigator.clipboard.writeText(link).then(() => {
                alert('✅ کانفیگ VLESS با موفقیت کپی شد!');
            }).catch(() => {
                alert('خطا در کپی کردن کانفیگ!');
            });
        }
function editUser(encodedUsername) {
    const username = decodeURIComponent(encodedUsername);
    const user = window.allUsers.find(u => u.username === username);
    if (!user) {
        alert('کاربر یافت نشد!');
        return;
    }
    isEditMode = true;
    editingUsername = username;
    document.getElementById('modal-title').innerText = 'ویرایش کاربر: ' + username;
    document.getElementById('submit-btn').innerText = 'ذخیره تغییرات';
    const nameInput = document.getElementById('input-name');
    nameInput.value = username;
    nameInput.disabled = false;
    document.getElementById('input-limit').value = user.limit_gb || '';
    document.getElementById('input-expiry').value = user.expiry_days || '';
    document.getElementById('input-req-limit').value = user.limit_req || '';
    document.getElementById('input-ip-limit').value = (user.ip_limit !== undefined && user.ip_limit !== null) ? user.ip_limit : (user.max_connections || '');
    document.getElementById('input-ips').value = user.ips || '';
    document.getElementById('fingerprint-select').value = user.fingerprint || 'chrome';
	document.getElementById('hidden-auto-rotate').value = user.auto_rotate_ip || '0';
	document.getElementById('hidden-rotate-time').value = user.rotate_time || '';
	document.getElementById('hidden-ip-operator').value = user.ip_operator || 'all';
	document.getElementById('hidden-ip-count').value = user.ip_count || '20';
    document.getElementById('fingerprint-select').value = user.fingerprint || 'chrome';
    document.getElementById('input-block-porn').checked = (user.block_porn === 1);
    document.getElementById('input-block-ads').checked = (user.block_ads === 1);
    const autoRotateUserProxyCheck = document.getElementById('input-auto-rotate-user-proxy');
    if (autoRotateUserProxyCheck) autoRotateUserProxyCheck.checked = (user.auto_rotate_user_proxy === 1);
    const hasAutoReset = Boolean((user.auto_reset_vol_days && user.auto_reset_vol_days > 0) || (user.auto_reset_req_days && user.auto_reset_req_days > 0));
    const autoResetToggle = document.getElementById('input-auto-reset-toggle');
    if (autoResetToggle) autoResetToggle.checked = hasAutoReset;
    document.getElementById('input-auto-reset-vol').value = hasAutoReset && user.auto_reset_vol_days > 0 ? user.auto_reset_vol_days : '';
    document.getElementById('input-auto-reset-req').value = hasAutoReset && user.auto_reset_req_days > 0 ? user.auto_reset_req_days : '';
    window.toggleAutoResetInputs(hasAutoReset);
    const hasFrag = Boolean(user.frag_len && user.frag_len !== "" && user.frag_int && user.frag_int !== "");
    const fragToggle = document.getElementById('input-frag-toggle');
    if (fragToggle) fragToggle.checked = hasFrag;
    document.getElementById('input-frag-len').value = hasFrag ? user.frag_len : '200-3000';
    document.getElementById('input-frag-int').value = hasFrag ? user.frag_int : '1-2';
    window.toggleFragInputs(hasFrag);
    const userPorts = String(user.port || '').split(',').map(p => p.trim());
    const predefinedPorts = [...tlsPorts, ...nonTlsPorts];
    const customPorts = userPorts.filter(p => !predefinedPorts.includes(p) && p !== '');
    document.querySelectorAll('input[name="ports"]').forEach(cb => {
        cb.checked = userPorts.includes(cb.value);
    });
    const customPortInput = document.getElementById('input-custom-ports');
    if (customPortInput) customPortInput.value = customPorts.join(' ');
    const userProxyToggle = document.getElementById('user-proxy-mode-toggle');
    const userLocSelect = document.getElementById('user-location-select');
    const userLocSearch = document.getElementById('user-location-search');
    const userSocksInput = document.getElementById('user-socks5-input');
    if (userLocSearch) {
        userLocSearch.value = '';
        if (typeof window.filterUserLocations === 'function') window.filterUserLocations();
    }
	const targetProxy = user.user_socks5 || user.user_proxy_ip;
	const userProxyResult = document.getElementById('test-user-proxy-result');
	if (userProxyResult) userProxyResult.innerText = '';
	if (targetProxy) {
		if (userProxyToggle) userProxyToggle.checked = true;
		if (typeof window.toggleUserProxyMode === 'function') window.toggleUserProxyMode(true);
		if (userSocksInput) userSocksInput.value = targetProxy;
		if (userLocSelect) userLocSelect.value = '';
	} else {
		if (userProxyToggle) userProxyToggle.checked = false;
		if (typeof window.toggleUserProxyMode === 'function') window.toggleUserProxyMode(false);
		if (userSocksInput) userSocksInput.value = '';
		if (userLocSelect) userLocSelect.value = user.user_proxy_iata || '';
	}
// بعد از پر کردن سایر فیلدها
const userProxyToggle = document.getElementById('user-proxy-mode-toggle');
const userSocksInput = document.getElementById('user-socks5-input');
const resultSpan = document.getElementById('test-user-proxy-result');
if (user.user_socks5) {
    if (userProxyToggle) userProxyToggle.checked = true;
    if (userSocksInput) userSocksInput.value = user.user_socks5;
    if (resultSpan) resultSpan.innerText = '';
    toggleUserProxyMode(true);
} else {
    if (userProxyToggle) userProxyToggle.checked = false;
    if (userSocksInput) userSocksInput.value = '';
    if (resultSpan) resultSpan.innerText = '';
    toggleUserProxyMode(false);
}
	toggleModal(true);
}
        async function deleteUser(encodedUsername) {
            const username = decodeURIComponent(encodedUsername);
            if (confirm('آیا از حذف کاربر ' + username + ' مطمئن هستید؟')) {
                try {
                    const response = await fetch('/api/users/' + encodeURIComponent(username), { method: 'DELETE' });
                    if (response.ok) {
                        alert('✅ کاربر با موفقیت حذف شد.');
                        await loadUsers(true);
                    } else {
                        const errData = await response.json();
                        alert('خطا: ' + (errData.error || 'عملیات ناموفق بود'));
                    }
                } catch (err) {
                    alert('خطا در برقراری ارتباط با سرور');
                }
            }
        }
        function getFlagEmoji(countryCode) {
            if (!countryCode) return '🌐';
            const codePoints = countryCode.toUpperCase().split('').map(char => 127397 + char.charCodeAt(0));
            try {
                return String.fromCodePoint(...codePoints);
            } catch (e) {
                return '🌐';
            }
        }
        function renderLocationsUI(locations, activeIata) {
            const select = document.getElementById('location-select');
            locations.sort((a, b) => (a.cca2 || '').localeCompare(b.cca2 || ''));
            let html = '<option value="">🌐 پیش‌فرض (لوکیشن خودکار)</option>';
            locations.forEach(loc => {
                if (loc.iata && loc.city) {
                    const flag = getFlagEmoji(loc.cca2);
                    const isSelected = loc.iata.toUpperCase() === activeIata.toUpperCase() ? 'selected' : '';
                    html += '<option value="' + loc.iata + '" ' + isSelected + '>' + flag + ' ' + loc.city + ' (' + loc.iata + ')</option>';
                }
            });
            select.innerHTML = html;
        }
        async function loadLocations() {
            const select = document.getElementById('location-select');
            const cachedLocations = localStorage.getItem('cached_locations_list');
            const cachedActiveIata = localStorage.getItem('cached_active_iata') || '';
            let hasCachedLocs = false;
            if (cachedLocations) {
                try {
                    const parsedLocs = JSON.parse(cachedLocations);
                    if (Array.isArray(parsedLocs) && parsedLocs.length > 0) {
                        renderLocationsUI(parsedLocs, cachedActiveIata);
                        hasCachedLocs = true;
                    }
                } catch(e) {}
            }
            try {
                const statusRes = await fetch('/api/proxy-ip');
                let activeIata = '';
                if (statusRes.ok) {
                    const statusData = await statusRes.json();
                    activeIata = statusData.iata || '';
                    localStorage.setItem('cached_active_iata', activeIata);
                    if(statusData.frag_len) {
                        window.globalFragLen = statusData.frag_len;
                        document.getElementById('frag-length').value = statusData.frag_len;
                    }
                    if(statusData.frag_int) {
                        window.globalFragInt = statusData.frag_int;
                        document.getElementById('frag-interval').value = statusData.frag_int;
                    }
                }
                const res = await fetch('/locations');
                if (!res.ok) throw new Error();
                const locations = await res.json();
                localStorage.setItem('cached_locations_list', JSON.stringify(locations));
                renderLocationsUI(locations, activeIata);
            } catch (err) {
                if (!hasCachedLocs) {
                    select.innerHTML = '<option value="">⚠️ خطا در دریافت لوکیشن‌ها</option>';
                }
            }
        }
        async function saveSettings() {
            const select = document.getElementById('location-select');
            const fragLen = document.getElementById('frag-length').value || "20-30";
            const fragInt = document.getElementById('frag-interval').value || "1-2";
            const iata = select.value;
            const btn = document.getElementById('save-settings-btn');
            btn.disabled = true;
            btn.innerText = 'در حال ذخیره...';
            try {
                let resolvedIp = 'proxyip.cmliussss.net';
                if (iata) {
                    const domain = iata.toLowerCase() + '.proxyip.cmliussss.net';
                    const dnsRes = await fetch('https://cloudflare-dns.com/dns-query?name=' + domain + '&type=A', {
                        headers: { 'accept': 'application/dns-json' }
                    });
                    resolvedIp = domain;
                    if (dnsRes.ok) {
                        const dnsData = await dnsRes.json();
                        if (dnsData.Answer && dnsData.Answer.length > 0) {
                            const ips = dnsData.Answer.filter(ans => ans.type === 1).map(ans => ans.data);
                            if (ips.length > 0) {
                                resolvedIp = ips[Math.floor(Math.random() * ips.length)];
                            }
                        }
                    }
                }
                const response = await fetch('/api/proxy-ip', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ proxy_ip: resolvedIp, iata: iata ? iata.toUpperCase() : '', frag_len: fragLen, frag_int: fragInt })
                });
                if (response.ok) {
                    window.globalFragLen = fragLen;
                    window.globalFragInt = fragInt;
                    alert('✅ تنظیمات با موفقیت ذخیره شد.\\n' + (iata ? 'آی‌پی پروکسی کلودفلر: ' + resolvedIp : 'آدرس پروکسی به حالت پیش‌فرض بازگشت.'));
                    toggleSettingsModal(false);
                } else {
                    alert('خطا در ذخیره تنظیمات');
                }
            } catch (err) {
                alert('خطا در برقراری ارتباط با سرور');
            } finally {
                btn.disabled = false;
                btn.innerText = 'ذخیره تنظیمات';
            }
        }
        function exportUsersBackup() {
            if (!window.allUsers || window.allUsers.length === 0) {
                alert('⚠️ کاربری برای پشتیبان‌گیری وجود ندارد!');
                return;
            }
            const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(window.allUsers, null, 2));
            const downloadAnchor = document.createElement('a');
            const dateStr = new Date().toISOString().split('T')[0];
            downloadAnchor.setAttribute("href", dataStr);
            downloadAnchor.setAttribute("download", "zeus_users_backup_" + dateStr + ".json");
            document.body.appendChild(downloadAnchor);
            downloadAnchor.click();
            downloadAnchor.remove();
        }
        function triggerImportBackup() {
            document.getElementById('backup-file-input').click();
        }
        async function importUsersBackup(event) {
            const file = event.target.files[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onload = async function(e) {
                const importBtn = document.querySelector('button[onclick="triggerImportBackup()"]');
                const exportBtn = document.querySelector('button[onclick="exportUsersBackup()"]');
                const closeBtn = document.querySelector('#settings-modal button[onclick="toggleSettingsModal(false)"]');
                try {
                    const backupUsers = JSON.parse(e.target.result);
                    if (!Array.isArray(backupUsers)) {
                        alert('❌ فایل پشتیبان نامعتبر است! ساختار فایل باید آرایه‌ای از کاربران باشد.');
                        return;
                    }
                    const validBackupUsers = backupUsers.filter(u => u && typeof u === 'object' && u.username);
                    if (validBackupUsers.length === 0) {
                        alert('❌ هیچ کاربر معتبری در فایل پشتیبان یافت نشد!');
                        return;
                    }
                    const existingUsernames = new Set((window.allUsers || []).map(u => u.username));
                    const duplicates = validBackupUsers.filter(u => existingUsernames.has(u.username));
                    let overwrite = false;
                    if (duplicates.length > 0) {
                        overwrite = confirm('⚠️ تعداد ' + duplicates.length + ' کاربر تکراری شناسایی شد. آیا می‌خواهید اطلاعات آن‌ها با فایل پشتیبان بازنویسی شود؟\\n(در صورت انتخاب لغو، کاربران تکراری نادیده گرفته می‌شوند)');
                    }
                    if (importBtn) importBtn.disabled = true;
                    if (exportBtn) exportBtn.disabled = true;
                    if (closeBtn) closeBtn.disabled = true;
                    let successCount = 0;
                    let currentStep = 0;
                    for (const u of validBackupUsers) {
                        currentStep++;
                        if (importBtn) {
                            importBtn.innerText = '⏳ بازیابی (' + currentStep + '/' + validBackupUsers.length + ')';
                        }
                        const exists = existingUsernames.has(u.username);
                        if (exists) {
                            if (overwrite) {
                                try {
                                    // Delete first
                                    await fetch('/api/users/' + encodeURIComponent(u.username), { method: 'DELETE' });
                                    // Post
                                    const res = await fetch('/api/users', {
                                        method: 'POST',
                                        headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({
                                            username: u.username,
                                            uuid: u.uuid,
                                            limit_gb: u.limit_gb,
                                            expiry_days: u.expiry_days,
                                            limit_req: u.limit_req,
                                            ips: u.ips,
                                            tls: u.tls,
                                            port: u.port,
                                            fingerprint: u.fingerprint,
                                            max_connections: u.max_connections,
                                            used_gb: u.used_gb,
                                            used_req: u.used_req,
                                            created_at: u.created_at,
                                            is_active: u.is_active
                                        })
                                    });
                                    if (res.ok) successCount++;
                                } catch(err) {}
                            }
                        } else {
                            try {
                                const res = await fetch('/api/users', {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({
                                        username: u.username,
                                        uuid: u.uuid,
                                        limit_gb: u.limit_gb,
                                        expiry_days: u.expiry_days,
                                        limit_req: u.limit_req,
                                        ips: u.ips,
                                        tls: u.tls,
                                        port: u.port,
                                        fingerprint: u.fingerprint,
                                        max_connections: u.max_connections,
                                        used_gb: u.used_gb,
                                        used_req: u.used_req,
                                        created_at: u.created_at,
                                        is_active: u.is_active
                                    })
                                });
                                if (res.ok) successCount++;
                            } catch(err) {}
                        }
                    }
                    alert('✅ عملیات بازیابی با موفقیت انجام شد. ' + successCount + ' کاربر بازیابی شدند.');
                    toggleSettingsModal(false);
                    await loadUsers(true);
                } catch(err) {
                    alert('❌ خطا در خواندن یا پردازش فایل پشتیبان!');
                } finally {
                    if (importBtn) {
                        importBtn.disabled = false;
                        importBtn.innerText = '📥 بارگذاری بک‌آپ';
                    }
                    if (exportBtn) exportBtn.disabled = false;
                    if (closeBtn) closeBtn.disabled = false;
                    event.target.value = '';
                }
            };
            reader.readAsText(file);
        }
        async function changeAdminPassword() {
            const currentPwd = document.getElementById('change-pwd-current').value;
            const newPwd = document.getElementById('change-pwd-new').value;
            const btn = document.getElementById('change-pwd-btn');
            if (!currentPwd || !newPwd) {
                alert('⚠️ وارد کردن رمز عبور فعلی و جدید الزامی است!');
                return;
            }
            if (newPwd.length < 4) {
                alert('⚠️ رمز عبور جدید باید حداقل ۴ کاراکتر باشد!');
                return;
            }
            btn.disabled = true;
            btn.innerText = 'در حال تغییر...';
            try {
                const response = await fetch('/api/change-password', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ current_password: currentPwd, new_password: newPwd })
                });
                const data = await response.json();
                if (response.ok && data.success) {
                    alert('✅ رمز عبور با موفقیت تغییر کرد.');
                    document.getElementById('change-pwd-current').value = '';
                    document.getElementById('change-pwd-new').value = '';
                    toggleSettingsModal(false);
                } else {
                    alert('❌ خطا: ' + (data.error || 'عملیات ناموفق بود'));
                }
            } catch (err) {
                alert('خطا در برقراری ارتباط با سرور');
            } finally {
                btn.disabled = false;
                btn.innerText = 'تغییر رمز عبور';
            }
        }
        async function logoutAdmin() {
            if (confirm('⚠️ آیا می‌خواهید از پنل خارج شوید؟')) {
                try {
                    await fetch('/api/logout', { method: 'POST' });
                } catch (err) {}
                window.location.reload();
            }
        }
const CURRENT_VERSION = '.MK';
const UPDATE_FIX = "constsCURRENT_VERSION='d.d.d'";
		async function checkForUpdates(isManual = false) {
            try {
                if (isManual) {
                    document.getElementById('update-toggle').classList.add('animate-pulse');
                }
                const res = await fetch('https://raw.githubusercontent.com/IR-NETLIFY/zeus/refs/heads/main/zeus.js?t=' + Date.now());
                if (!res.ok) throw new Error('Network response was not ok');
                const text = await res.text();
                const match = text.match(/const\\s+CURRENT_VERSION\\s*=\\s*['"](\\d+\\.\\d+\\.\\d+)['"]/i);
                const latestVersion = match ? match[1] : null;
                if (isManual) {
                    document.getElementById('update-toggle').classList.remove('animate-pulse');
                }
                if (latestVersion && latestVersion !== CURRENT_VERSION) {
                    document.getElementById('update-toggle').className = "p-2 rounded-lg bg-red-100 dark:bg-red-900/60 border border-red-500 hover:bg-red-200 dark:hover:bg-red-900/80 transition text-red-700 dark:text-red-400 shadow-[0_0_15px_rgba(239,68,68,0.6)] animate-pulse relative";
                    const badge = document.getElementById('update-badge');
                    if (badge) badge.remove();
                    if (isManual) {
                        // باز کردن پنجره اختصاصی آپدیت به جای alert معمولی
                        toggleUpdateModal(true, latestVersion);
                    }
                } else {
                    if (isManual) {
                        alert('شما در حال استفاده از آخرین نسخه (v' + CURRENT_VERSION + ') هستید.');
                    }
                }
            } catch (err) {
                if (isManual) {
                    document.getElementById('update-toggle').classList.remove('animate-pulse');
                    alert('خطا در بررسی آپدیت از گیت هاب.');
                }
            }
        }
        async function applyUpdate() {
            // بستن پنجره آپدیت قبل از شروع عملیات
            toggleUpdateModal(false);
            const btn = document.getElementById('update-toggle');
            btn.disabled = true;
            alert('در حال دریافت و اعمال آپدیت... لطفاً چند ثانیه صبر کنید.');
            try {
                const res = await fetch('/api/update-panel', { method: 'POST' });
                const data = await res.json();
                if (res.ok && data.success) {
                    alert('پنل با موفقیت به آخرین نسخه آپدیت شد! در حال راه‌اندازی مجدد پنل (لطفاً ۵ ثانیه صبر کنید)...');
                    setTimeout(() => {
                        window.location.reload();
                    }, 5000);
                } else {
                    alert('خطا در بروزرسانی. لطفاً با استفاده از دکمه "آپدیت دستی" اقدام کنید.');
                    btn.disabled = false;
                }
            } catch (err) {
                alert('خطا در ارتباط با سرور. لطفاً از گزینه آپدیت دستی استفاده کنید.');
                btn.disabled = false;
            }
        }
let cachedIpsData = {};
async function fetchIpsList() {
    try {
        const response = await fetch('https://raw.githubusercontent.com/IR-NETLIFY/zeus/refs/heads/main/ips.txt');
        if (!response.ok) throw new Error('Fetch failed');
        const text = await response.text();
        const blocks = text.split('----------');
        cachedIpsData = {};
        blocks.forEach(block => {
            const lines = block.trim().split('\\n').map(l => l.trim()).filter(l => l.length > 0);
            if (lines.length === 0) return;
            let opName = "Unknown";
            const ips = [];
            lines.forEach(line => {
                if (line.includes('#')) {
                    opName = line.split('#')[1].trim();
                } else if (!line.startsWith('[source')) {
                    ips.push(line);
                }
            });
            if (ips.length > 0) {
                cachedIpsData[opName] = ips;
            }
        });
        populateIpSelect();
    } catch (err) {
        alert('Failed to load IP list from GitHub.');
        toggleIpSelectorModal(false);
    }
}
function populateIpSelect() {
    const select = document.getElementById('ip-operator-select');
    select.innerHTML = '<option value="all">All</option>';
    Object.keys(cachedIpsData).forEach(op => {
        const option = document.createElement('option');
        option.value = op;
        option.textContent = op;
        select.appendChild(option);
    });
}
function toggleIpSelectorModal(show) {
    const modal = document.getElementById('ip-selector-modal');
    const card = modal.querySelector('div');
    if (show) {
        modal.classList.remove('opacity-0', 'pointer-events-none');
        modal.classList.add('opacity-100', 'pointer-events-auto');
        card.classList.remove('opacity-0', 'scale-95');
        card.classList.add('opacity-100', 'scale-100');
    } else {
        modal.classList.remove('opacity-100', 'pointer-events-auto');
        modal.classList.add('opacity-0', 'pointer-events-none');
        card.classList.remove('opacity-100', 'scale-100');
        card.classList.add('opacity-0', 'scale-95');
    }
}
// ===== توابع مربوط به پروکسی اختصاصی =====
window.toggleUserProxyMode = function(isSocksMode) {
    const container = document.getElementById('user-socks5-container');
    const input = document.getElementById('user-socks5-input');
    const result = document.getElementById('test-user-proxy-result');
    if (isSocksMode) {
        container.classList.remove('opacity-50', 'pointer-events-none');
        input.disabled = false;
    } else {
        container.classList.add('opacity-50', 'pointer-events-none');
        input.disabled = true;
        input.value = '';
        if (result) result.innerText = '';
    }
};

async function testUserSocksProxy() {
    const btn = document.getElementById('test-user-proxy-btn');
    const resultSpan = document.getElementById('test-user-proxy-result');
    const proxyStr = document.getElementById('user-socks5-input').value.trim();
    if (!proxyStr) {
        resultSpan.innerText = '⚠️ آدرس پروکسی را وارد کنید!';
        resultSpan.className = 'text-[11px] font-bold text-red-500 w-full mt-1';
        return;
    }
    btn.disabled = true;
    btn.innerText = '⏳';
    resultSpan.innerText = '';
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);
    try {
        const res = await fetch('/api/test-proxy', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ proxy: proxyStr }),
            signal: controller.signal
        });
        clearTimeout(timeoutId);
        const data = await res.json();
        if (res.ok && data.success) {
            const flag = typeof getFlagEmoji === 'function' ? getFlagEmoji(data.country) : '🌐';
            resultSpan.innerText = flag + ' ✅ پینگ: ' + data.ping + 'ms';
            resultSpan.className = 'text-[11px] font-bold text-green-600 w-full mt-1';
        } else {
            resultSpan.innerText = '❌ ' + (data.error || 'ناموفق');
            resultSpan.className = 'text-[11px] font-bold text-red-500 w-full mt-1 break-words';
        }
    } catch (e) {
        clearTimeout(timeoutId);
        if (e.name === 'AbortError') resultSpan.innerText = '⏱️ تایم‌اوت (پروکسی پاسخ نداد)';
        else resultSpan.innerText = '❌ خطا در ارتباط';
        resultSpan.className = 'text-[11px] font-bold text-red-500 w-full mt-1 break-words';
    } finally {
        btn.disabled = false;
        btn.innerText = 'تست پروکسی';
    }
}
async function openIpSelectorModal() {
    toggleIpSelectorModal(true);
    document.getElementById('ip-loading-state').classList.remove('hidden');
    document.getElementById('ip-selection-form').classList.add('hidden');
    await fetchIpsList();
    document.getElementById('ip-loading-state').classList.add('hidden');
    document.getElementById('ip-selection-form').classList.remove('hidden');
}
function applySelectedIps() {
    const operator = document.getElementById('ip-operator-select').value;
    let count = parseInt(document.getElementById('ip-count-input').value, 10);
    if (isNaN(count) || count < 1) count = 10;
    let availableIps = [];
    if (operator === 'all') {
        Object.values(cachedIpsData).forEach(ips => {
            availableIps = availableIps.concat(ips);
        });
    } else {
        availableIps = cachedIpsData[operator] || [];
    }
    availableIps = [...new Set(availableIps)];
    let selectedIps = [];
    if (count >= availableIps.length) {
        selectedIps = availableIps;
    } else {
        const shuffled = availableIps.slice();
        for (let i = shuffled.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
        }
        selectedIps = shuffled.slice(0, count);
    }
    document.getElementById('input-ips').value = selectedIps.join('\\n');
    toggleIpSelectorModal(false);
}
document.addEventListener('DOMContentLoaded', () => {
        if (localStorage.getItem('zeus_path_warned_' + CURRENT_VERSION) !== 'true') {
            const modal = document.getElementById('path-warning-modal');
            const card = modal.querySelector('div');
            modal.classList.remove('opacity-0', 'pointer-events-none');
            modal.classList.add('opacity-100', 'pointer-events-auto');
            card.classList.remove('opacity-0', 'scale-95');
            card.classList.add('opacity-100', 'scale-100');
        }			
            const versionBadge = document.getElementById('panel-version');
            if (versionBadge) versionBadge.innerText = 'v' + CURRENT_VERSION;
            renderPortCheckboxes();
            loadUsers();
            loadLocations();
            setInterval(() => loadUsers(true), 60000);
            setTimeout(() => checkForUpdates(false), 2000);
        });
    </script>
</body>
</html>`,
	 status: `<html lang="fa" dir="rtl" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>وضعیت اشتراک کاربر</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css" rel="stylesheet" type="text/css" />

    <div id="toast-container" class="fixed bottom-5 left-1/2 -translate-x-1/2 z-[9999] flex flex-col gap-2 pointer-events-none"></div>
    
    <script>
        // showToast و tailwind.config (همان کد اول)
        function showToast(message) {
            const container = document.getElementById('toast-container');
            const toast = document.createElement('div');
            toast.className = 'px-5 py-3 bg-amber-500/90 backdrop-blur-md border border-amber-400/50 text-white rounded-full shadow-[0_4px_20px_rgba(245,158,11,0.4)] font-bold text-xs transform transition-all duration-500 translate-y-full opacity-0 whitespace-nowrap';
            toast.innerText = message;
            container.appendChild(toast);
            requestAnimationFrame(() => {
                toast.classList.remove('translate-y-full', 'opacity-0');
            });
            setTimeout(() => {
                toast.classList.add('translate-y-full', 'opacity-0');
                setTimeout(() => toast.remove(), 500);
            }, 3000);
        }

        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    fontFamily: { sans: ['Vazirmatn', 'sans-serif'] },
                    colors: { amoled: { bg: '#100904ff', card: '#120b08', input: '#19110d', border: '#2d1e16' } }
                }
            }
        }
    </script>

    <style>
        body { font-family: 'Vazirmatn', sans-serif; transition: all 0.4s ease; }
        .glass { backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px); background: rgba(255, 255, 255, 0.05); border: 1px solid rgba(255, 255, 255, 0.08); }
        .dark .glass { background: rgba(0, 0, 0, 0.4); border-color: rgba(255, 255, 255, 0.06); }
        .glow-gold { box-shadow: 0 0 30px rgba(245, 158, 11, 0.15); }
        .stat-card { transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); }
        .stat-card:hover { transform: translateY(-4px) scale(1.01); }
        .progress-ring { transition: stroke-dashoffset 0.8s ease; }
        .badge-pulse { animation: pulse 2s infinite; }
        @keyframes pulse { 0% { opacity: 0.6; } 50% { opacity: 1; } 100% { opacity: 0.6; } }
        .toast-container { position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%); z-index: 9999; display: flex; flex-direction: column; gap: 8px; pointer-events: none; }
        .toast { padding: 12px 24px; border-radius: 40px; font-weight: bold; font-size: 0.9rem; backdrop-filter: blur(12px); background: rgba(245, 158, 11, 0.9); color: #fff; box-shadow: 0 8px 30px rgba(245, 158, 11, 0.3); border: 1px solid rgba(255, 255, 255, 0.15); transform: translateY(20px); opacity: 0; transition: all 0.5s ease; pointer-events: auto; }
        .toast.show { transform: translateY(0); opacity: 1; }
    </style>
</head>
<body class="min-h-screen bg-gradient-to-br from-amber-50 to-orange-50 dark:from-[#0a0806] dark:to-[#1a120e] text-stone-800 dark:text-zinc-100 flex flex-col items-center justify-between p-4 md:p-8 font-sans antialiased selection:bg-amber-200 dark:selection:bg-zinc-800 transition-colors duration-300" id="main-body">


    <div class="w-full max-w-2xl glass rounded-3xl shadow-2xl p-6 md:p-8 relative overflow-hidden glow-gold">
        <!-- decorative blobs -->
        <div class="absolute -top-20 -left-20 w-64 h-64 bg-amber-500/10 rounded-full blur-3xl pointer-events-none"></div>
        <div class="absolute -bottom-20 -right-20 w-64 h-64 bg-orange-500/10 rounded-full blur-3xl pointer-events-none"></div>

<!-- آیکون ماهواره (حرفه‌ای و با جزئیات) -->
<div class="flex justify-center mb-2.5">
    <svg class="w-12 h-12 md:w-16 md:h-16 text-amber-500 dark:text-amber-400 sunset:text-amber-400 drop-shadow-lg" 
         fill="none" 
         stroke="currentColor" 
         viewBox="0 0 24 24" 
         stroke-width="1.5" 
         stroke-linecap="round" 
         stroke-linejoin="round">
        
        <!-- بدنه اصلی ماهواره (مکعب با گوشه‌های گرد) -->
        <rect x="8" y="8" width="8" height="8" rx="1.5" />
        
        <!-- آنتن اصلی (بالای ماهواره) -->
        <path d="M12 6v2" />
        <circle cx="12" cy="5" r="0.8" fill="currentColor" />
        
        <!-- آنتن فرعی (کناری) -->
        <path d="M16 12h2" />
        <circle cx="18.5" cy="12" r="0.8" fill="currentColor" />
        
        <!-- پنل خورشیدی چپ -->
        <rect x="4" y="10" width="4" height="4" rx="0.5" />
        <line x1="5" y1="11" x2="7" y2="11" stroke-width="0.5" />
        <line x1="5" y1="12" x2="7" y2="12" stroke-width="0.5" />
        <line x1="5" y1="13" x2="7" y2="13" stroke-width="0.5" />
        
        <!-- پنل خورشیدی راست -->
        <rect x="16" y="10" width="4" height="4" rx="0.5" />
        <line x1="17" y1="11" x2="19" y2="11" stroke-width="0.5" />
        <line x1="17" y1="12" x2="19" y2="12" stroke-width="0.5" />
        <line x1="17" y1="13" x2="19" y2="13" stroke-width="0.5" />
        
        <!-- آنتن دیش (زیر ماهواره) -->
        <path d="M12 16v2" />
        <path d="M10 18l2 2 2-2" />
        
        <!-- نقاط نورانی (LED indicators) روی بدنه -->
        <circle cx="10.5" cy="10.5" r="0.5" fill="currentColor" opacity="0.7" />
        <circle cx="13.5" cy="10.5" r="0.5" fill="currentColor" opacity="0.7" />
        <circle cx="10.5" cy="13.5" r="0.5" fill="currentColor" opacity="0.7" />
        <circle cx="13.5" cy="13.5" r="0.5" fill="currentColor" opacity="0.7" />
    </svg>
</div>

        <!-- header -->
        <div class="text-center mb-8 relative z-10">
                <h1 class="text-2xl font-bold tracking-tight text-amber-950 dark:text-white mb-1">
        <span class="text-white dark:text-white">اشتراک </span>
        <span class="text-amber-500 dark:text-amber-400 sunset:text-amber-400">ما</span>
        <span class="text-white dark:text-white">که</span>
        <span class="text-amber-500 dark:text-amber-400 sunset:text-amber-400">وصلیم🚀</span>
    </h1>
                <div class="inline-block px-3 py-1.5 mt-1 border border-amber-500/30 dark:border-amber-400/30 rounded-xl bg-white/10 dark:bg-zinc-900/30 backdrop-blur-sm shadow-sm">
        <p class="text-sm md:text-base font-mono font-semibold text-amber-600 dark:text-amber-400 tracking-wide" id="display-username"></p>
    </div>
            <div id="live-connections-badge" class="hidden inline-flex items-center gap-2.5 px-4 py-1.5 mt-3 bg-amber-500/10 border-2 border-amber-500/30 text-amber-500 rounded-full text-xs font-bold shadow-md backdrop-blur-sm transition-all duration-300 hover:scale-105">
                <span class="w-2 h-2 rounded-full bg-amber-500 animate-pulse"></span>
                <span id="live-connections-text">۰ دستگاه متصل</span>
            </div>
        </div>


        <!-- stats grid - only 3 cards: volume, expiry, online -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-5 mb-8 relative z-10">
            <!-- volume card -->
            <div class="stat-card bg-white/60 dark:bg-zinc-900/40 border border-amber-200/50 dark:border-amber-800/20 rounded-2xl p-5 shadow-sm hover:shadow-md backdrop-blur-sm">
                <div class="flex items-center justify-between mb-2">
                    <span class="text-xs font-semibold text-amber-800 dark:text-amber-300 flex items-center gap-1.5">
                                    <svg class="w-4 h-4 text-amber-500" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
                <ellipse cx="12" cy="5" rx="9" ry="3" />
                <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
                <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
            </svg>
                        حجم 📊
                    </span>
                    <span id="volume-pct" class="text-xs font-bold text-amber-600 dark:text-amber-400">۰٪</span>
                </div>
                <div class="w-full bg-amber-200/50 dark:bg-amber-950/40 rounded-full h-2.5 overflow-hidden mb-3">
                    <div id="volume-progress" class="bg-gradient-to-r from-amber-500 to-orange-500 h-2.5 rounded-full transition-all duration-1000" style="width: 0%"></div>
                </div>
                <div class="flex justify-between text-xs text-amber-800 dark:text-amber-300 font-medium">
                    <span>مصرف شده: <span id="used-vol" class="font-bold text-amber-950 dark:text-amber-100">-</span></span>
                    <span>حجم کل: <span id="limit-vol" class="font-bold text-amber-950 dark:text-amber-100">-</span></span>
                </div>
            </div>

            <!-- expiry card -->
            <div class="stat-card bg-white/60 dark:bg-zinc-900/40 border border-amber-200/50 dark:border-amber-800/20 rounded-2xl p-5 shadow-sm hover:shadow-md backdrop-blur-sm">
                <div class="flex items-center justify-between mb-2">
                    <span class="text-xs font-semibold text-amber-800 dark:text-amber-300 flex items-center gap-1.5">
                        <svg class="w-4 h-4 text-amber-500" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
    <rect x="3" y="4" width="18" height="18" rx="2" ry="2" />
    <line x1="16" y1="2" x2="16" y2="6" />
    <line x1="8" y1="2" x2="8" y2="6" />
    <line x1="3" y1="10" x2="21" y2="10" />
</svg>
                        زمان 📊
                    </span>
                    <span id="expiry-pct" class="text-xs font-bold text-amber-600 dark:text-amber-400">۰٪</span>
                </div>
                <div class="w-full bg-amber-200/50 dark:bg-amber-950/40 rounded-full h-2.5 overflow-hidden mb-3">
                    <div id="expiry-progress" class="bg-gradient-to-r from-amber-400 to-amber-600 h-2.5 rounded-full transition-all duration-1000" style="width: 0%"></div>
                </div>
                <div class="flex justify-between text-xs text-amber-800 dark:text-amber-300 font-medium">
                    <span>باقی‌مانده: <span id="days-remaining" class="font-bold text-amber-950 dark:text-amber-100">-</span></span>
                    <span>کل اعتبار: <span id="total-days" class="font-bold text-amber-950 dark:text-amber-100">-</span></span>
                </div>
            </div>

        <!-- action buttons -->
        <div class="border-t border-amber-200/30 dark:border-zinc-800/50 pt-6 relative z-10">
            <h2 class="text-sm font-bold mb-4 flex items-center gap-2 text-amber-800 dark:text-amber-300">
                <svg class="w-4 h-4 text-amber-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/></svg>
                دریافت کانفیگ ها و ساب متنی
            </h2>
            <div class="space-y-3">
                <button onclick="copyTextSub()" class="w-full flex justify-between items-center px-5 py-3.5 bg-white/70 dark:bg-zinc-900/50 border border-amber-200/50 dark:border-amber-800/20 hover:border-amber-500 dark:hover:border-amber-500 rounded-xl text-sm font-medium transition-all duration-200 shadow-sm hover:shadow-md backdrop-blur-sm group">
                    <span class="flex items-center gap-2 text-amber-800 dark:text-amber-300 group-hover:text-amber-600 dark:group-hover:text-amber-400">⚡ کپی لینک ساب متنی برای نرم افزار</span>
                    <span class="text-amber-500 text-xs font-bold">کپی</span>
                </button>
                <button onclick="copyVlessConfig()" class="w-full flex justify-between items-center px-5 py-3.5 bg-white/70 dark:bg-zinc-900/50 border border-amber-200/50 dark:border-amber-800/20 hover:border-orange-500 dark:hover:border-orange-500 rounded-xl text-sm font-medium transition-all duration-200 shadow-sm hover:shadow-md backdrop-blur-sm group">
                    <span class="flex items-center gap-2 text-amber-800 dark:text-amber-300 group-hover:text-orange-600 dark:group-hover:text-orange-400">🛰️ کپی تمامی کانفیگ ها (مستقیم)</span>
                    <span class="text-orange-500 text-xs font-bold">کپی</span>
                </button>
            </div>
        </div>
    </div>
</div>


    <!-- footer -->
    <div class="flex items-center justify-center gap-4 mt-6 z-10 flex-wrap">
        <a href="https://t.me/makevaslim4" target="_blank" class="flex items-center gap-2 px-4 py-2 bg-white/70 dark:bg-zinc-900/50 border border-amber-200/50 dark:border-amber-800/20 rounded-full shadow-sm hover:shadow-md transition text-sm font-bold text-amber-800 dark:text-amber-300 hover:text-amber-600 dark:hover:text-amber-400 group backdrop-blur-sm">
            <svg class="w-5 h-5 text-amber-500 group-hover:scale-110 transition" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-1.13 7.19-.14.75-.42 1-.68 1.03-.58.05-1.02-.38-1.58-.75-.88-.58-1.38-.94-2.23-1.5-.99-.65-.35-1.01.22-1.59.15-.15 2.71-2.48 2.76-2.69a.2.2 0 00-.05-.18c-.06-.05-.14-.03-.21-.02-.09.02-1.49.94-4.22 2.79-.4.27-.76.41-1.08.4-.36-.01-1.04-.2-1.55-.37-.63-.2-1.12-.31-1.08-.66.02-.18.27-.36.74-.55 2.92-1.27 4.86-2.11 5.83-2.51 2.78-1.16 3.35-1.36 3.73-1.37.08 0 .27.02.39.12.1.08.13.19.14.27-.01.06.01.24 0 .24z"/></svg>
            Makevaslim
        </a>
    </div>

    <div class="text-xs text-zinc-500 dark:text-zinc-500 mt-5 text-center border-t border-zinc-900/10 dark:border-zinc-800/50 pt-4 opacity-80 select-none">
        طراحی شده توسط تیم 
        <span class="inline-block align-middle mx-1 font-bold text-amber-700 dark:text-amber-400">ماکه‌وصلیم</span>
        <span class="inline-block align-middle mx-1 text-amber-500">🖤19🖤18🖤</span>
    </div>

    <!-- toast container -->
    <div class="toast-container" id="toast-container"></div>


    <script>



        /* {{USER_DATA_PLACEHOLDER}} */



        function getHost() {



            return window.location.host;



        }



        function getVlessLink() {



            const u = window.statusUser;



            const host = getHost();



            var ips = [host];



            if (u.ips) {



                ips = u.ips.split('\\n').map(function(ip) { return ip.trim(); }).filter(function(ip) { return ip.length > 0; });



                if (ips.length === 0) ips = [host];



            }



            var ports = String(u.port || '443').split(',').map(function(p) { return p.trim(); }).filter(function(p) { return p.length > 0; });



            var fp = u.fingerprint || 'chrome';



            var links = [];



            ips.forEach(function(ip, ipIndex) {



                ports.forEach(function(portStr) {



                    var isTlsPort = ['443', '2053', '2083', '2087', '2096', '8443'].includes(portStr);



                    var tlsVal = isTlsPort ? 'tls' : 'none';



                    var remark = ips.length > 1 ? (u.username + '-' + (ipIndex + 1) + '-' + portStr) : (u.username + '-' + portStr);



                    links.push('vle' + 'ss://' + (u.uuid || '') + '@' + ip + ':' + portStr + '?path=%2FMa_Ke_Vaslim&security=' + tlsVal + '&encryption=none&insecure=0&host=' + host + '&fp=' + fp + '&type=ws&allowInsecure=0&sni=' + host + '#' + encodeURIComponent(remark));



                });



            });



            return links.join('\\n');



        }



        // جایگزینی کدهای قدیمی کپی ساب‌اسکریپشن و VLESS

async function copyVlessConfig() {
    try {
        const subUrl = window.location.protocol + '//' + getHost() + '/sub/' + encodeURIComponent(window.statusUser.username);
        const response = await fetch(subUrl);
        const allConfigs = await response.text();
        await navigator.clipboard.writeText(allConfigs);
        // استفاده از Toast جدید به جای alert
        showToast('✅ همه کانفیگ‌ها با موفقیت کپی شدند، ماکه‌وصلیم.');
    } catch (error) {
        showToast('❌ خطا در دریافت کانفیگ‌ها از سرور');
        console.error(error);
    }
}

function copyTextSub() {
    const link = window.location.protocol + '//' + getHost() + '/sub/' + encodeURIComponent(window.statusUser.username);
    navigator.clipboard.writeText(link).then(() => {
        // استفاده از Toast جدید به جای alert
        showToast('✅ لینک ساب متنی کپی شد، ماکه‌وصلیم.');
    }).catch(() => {
        showToast('❌ خطا در کپی لینک ساب!');
    });
}



        document.addEventListener('DOMContentLoaded', () => {



            const u = window.statusUser;



            if (!u) return;



            document.getElementById('display-username').innerText = u.username;



            const badge = document.getElementById('live-connections-badge');



            badge.classList.remove('hidden');



            if (u.online_count && u.online_count > 0) {



                document.getElementById('live-connections-text').innerText = u.online_count + (u.max_connections ? '/' + u.max_connections : '') + ' دستگاه متصل';



                badge.className = 'inline-flex items-center gap-1.5 px-3 py-1 bg-amber-500/10 border border-amber-500/20 text-amber-500 rounded-full text-xs font-bold shadow-sm';



                badge.querySelector('span.w-2').className = 'w-2 h-2 rounded-full bg-amber-500 animate-pulse';



            } else {



                document.getElementById('live-connections-text').innerText = '۰ دستگاه متصل';



                badge.className = 'inline-flex items-center gap-1.5 px-3 py-1 bg-gray-500/10 border border-gray-500/20 text-gray-500 dark:text-zinc-400 rounded-full text-xs font-bold shadow-sm';



                badge.querySelector('span.w-2').className = 'w-2 h-2 rounded-full bg-gray-500';



            // بارگذاری خودکار و تبدیل آنی به فرمت متنی Vless درون کادر

const subUrlText = window.location.protocol + '//' + getHost() + '/sub/' + encodeURIComponent(u.username);

fetch(subUrlText)

    .then(res => res.text())

    .then(rawData => {

        // تبدیل خودکار خروجی سرور به لینک‌های Vless

        document.getElementById('configs-text-area').value = decodeSubToVless(rawData);

    })

    .catch(err => {

        document.getElementById('configs-text-area').value = '❌ خطا در بارگذاری متنی کانفیگ‌ها از سرور';

        console.error(err);

    });

    

                }



            // Compute volume



            const usedGb = u.used_gb || 0;



            const limitGb = u.limit_gb;



            const formattedUsed = usedGb < 1 ? (usedGb * 1024).toFixed(0) + ' MB' : usedGb.toFixed(2) + ' GB';



            document.getElementById('used-vol').innerText = formattedUsed;



            let isVolumeExpired = false;



            if (limitGb) {



                document.getElementById('limit-vol').innerText = limitGb + ' GB';



                const pct = Math.min((usedGb / limitGb) * 100, 100);



                document.getElementById('volume-pct').innerText = pct.toFixed(0) + '٪';



                document.getElementById('volume-progress').style.width = pct + '%';



                // Color bar



                const hue = 35 - (pct * 0.35);



                document.getElementById('volume-progress').style.backgroundColor = 'hsl(' + hue + ', 85%, 45%)';



                if (usedGb >= limitGb) isVolumeExpired = true;



            } else {



                document.getElementById('limit-vol').innerText = 'نامحدود';



                document.getElementById('volume-pct').innerText = '۰٪';



                document.getElementById('volume-progress').style.width = '100%';



                document.getElementById('volume-progress').style.backgroundColor = '#f97316';



            }



            // Compute Expiry



            let daysRemaining = 'نامحدود';



            let totalDays = 'نامحدود';



            let isTimeExpired = false;



            if (u.expiry_days) {



                totalDays = u.expiry_days + ' روز';



                if (u.created_at) {



                    const created = new Date(u.created_at);



                    const expiryDate = new Date(created.getTime() + (u.expiry_days * 24 * 60 * 60 * 1000));



                    const diffDays = Math.ceil((expiryDate - new Date()) / (1000 * 60 * 60 * 24));



                    daysRemaining = diffDays > 0 ? diffDays : 0;



                    const pct = Math.max(0, Math.min(100, (daysRemaining / u.expiry_days) * 100));



                    document.getElementById('expiry-pct').innerText = pct.toFixed(0) + '٪';



                    document.getElementById('expiry-progress').style.width = pct + '%';



                    const hue = pct * 0.35;



                    document.getElementById('expiry-progress').style.backgroundColor = 'hsl(' + hue + ', 85%, 45%)';



                    if (new Date() > expiryDate) isTimeExpired = true;



                }



            } else {



                document.getElementById('expiry-pct').innerText = '۰٪';



                document.getElementById('expiry-progress').style.width = '100%';



                document.getElementById('expiry-progress').style.backgroundColor = '#eab308';



            }



            document.getElementById('days-remaining').innerText = daysRemaining === 'نامحدود' ? 'نامحدود' : daysRemaining + ' روز';



            document.getElementById('total-days').innerText = totalDays;



            const usedReq = u.used_req || 0;



            const limitReq = u.limit_req;



            document.getElementById('used-req').innerText = usedReq.toLocaleString();



            let isReqExpired = false;



            if (limitReq) {



                document.getElementById('limit-req').innerText = limitReq.toLocaleString();



                const rPct = Math.min((usedReq / limitReq) * 100, 100);



                document.getElementById('req-pct').innerText = rPct.toFixed(0) + '٪';



                document.getElementById('req-progress').style.width = rPct + '%';



                const rHue = 35 - (rPct * 0.35);



                document.getElementById('req-progress').style.backgroundColor = 'hsl(' + rHue + ', 85%, 45%)';



                if (usedReq >= limitReq) isReqExpired = true;



            } else {



                document.getElementById('limit-req').innerText = 'نامحدود';



                document.getElementById('req-pct').innerText = '۰٪';



                document.getElementById('req-progress').style.width = '100%';



                document.getElementById('req-progress').style.backgroundColor = '#f97316';



            }



            const onlineCount = u.online_count || 0;



            const maxConns = u.max_connections;



            document.getElementById('online-count').innerText = onlineCount;



            if (maxConns) {



                document.getElementById('limit-online').innerText = maxConns;



                const oPct = Math.min((onlineCount / maxConns) * 100, 100);



                document.getElementById('online-pct').innerText = oPct.toFixed(0) + '٪';



                document.getElementById('online-progress').style.width = oPct + '%';



                const oHue = 35 - (oPct * 0.35);



                document.getElementById('online-progress').style.backgroundColor = 'hsl(' + oHue + ', 85%, 45%)';



            } else {



                document.getElementById('limit-online').innerText = 'نامحدود';



                document.getElementById('online-pct').innerText = '۰٪';



                document.getElementById('online-progress').style.width = '100%';



                document.getElementById('online-progress').style.backgroundColor = onlineCount > 0 ? '#ea580c' : '#78716c'; 



            }



            const statusCard = document.getElementById('status-card');



            const statusText = document.getElementById('status-text');



            if (u.is_active === 0) {



                statusCard.className = 'mb-6 rounded-2xl p-4 text-center border font-bold relative z-10 bg-red-500/10 border-red-500/30 text-red-500 shadow-md shadow-red-500/5';



                statusCard.style.boxShadow = 'inset 0 0 12px rgba(239, 68, 68, 0.1)';



                statusText.innerText = '❌ وضعیت اشتراک: غیرفعال / مسدود دستی';



            } else if (isVolumeExpired) {



                statusCard.className = 'mb-6 rounded-2xl p-4 text-center border font-bold relative z-10 bg-orange-500/10 border-orange-500/30 text-orange-500 shadow-md shadow-orange-500/5';



                statusText.innerText = '⚠️ وضعیت اشتراک: تمام شدن حجم مجاز';



            } else if (isReqExpired) {



                statusCard.className = 'mb-6 rounded-2xl p-4 text-center border font-bold relative z-10 bg-orange-500/10 border-orange-500/30 text-orange-500 shadow-md shadow-orange-500/5';



                statusText.innerText = '⚠️ وضعیت اشتراک: تمام شدن ریکوئست مجاز';



            } else if (isTimeExpired) {



                statusCard.className = 'mb-6 rounded-2xl p-4 text-center border font-bold relative z-10 bg-orange-500/10 border-orange-500/30 text-orange-500 shadow-md shadow-orange-500/5';



                statusText.innerText = '⏳ وضعیت اشتراک: منقضی شده (پایان زمان اعتبار)';



            } else {



                statusCard.className = 'mb-6 rounded-2xl p-4 text-center border font-bold relative z-10 bg-amber-500/10 border-amber-500/30 text-amber-500 shadow-md shadow-amber-500/5';



                statusText.innerText = '✅ وضعیت اشتراک: فعال و متصل';



            }



        });

// تابع اختصاصی تغییر تم داینامیک فایل اول
        function setTheme(themeName) {
    const body = document.getElementById('main-body');
    if (!body) return;
    
    // حذف تم‌های قبلی (کلمه 'theme-day' حذف شد)
    body.classList.remove('theme-sunset', 'theme-night', 'bg-amber-50', 'dark:bg-amoled-bg', 'text-stone-800', 'dark:text-zinc-100');
    body.classList.add('theme-' + themeName);
    
    // مدیریت رنگ نوشته‌ها متناسب با هر تم (شرط مربوط به day حذف شد)
    if (themeName === 'night') {
        body.style.color = '#93c5fd';
    } else {
        body.style.color = '#fbd38d';
    }
}

    </script>



</body>
</html>`,
}; 
