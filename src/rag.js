import { GoogleGenAI } from "@google/genai";

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const pathname = url.pathname;
    const method = request.method.toUpperCase();

    // Initialize Gemini SDK client
    const initAI = (apiKey) => new GoogleGenAI({ apiKey });

    // KV helpers
    async function load() {
      const raw = await env.RAG.get("stores");
      return raw ? JSON.parse(raw) : { file_stores: {}, current_store_name: null };
    }
    async function save(data) {
      await env.RAG.put("stores", JSON.stringify(data));
    }

    // sanitize filename
    const cleanFilename = (name) =>
      (name || "file").trim().replace(/\s+/g, "_").replace(/[^A-Za-z0-9_\-\.]/g, "_").substring(0, 180);

    // detect mime-type by extension
    function detectMimeType(filename, fallback = "application/octet-stream") {
      if (!filename) return fallback;
      const ext = filename.split(".").pop().toLowerCase();
      const MIME_MAP = {
        pdf: "application/pdf",
        txt: "text/plain",
        md: "text/markdown",
        json: "application/json",
        csv: "text/csv",
        tsv: "text/tab-separated-values",
        xml: "application/xml",
        yaml: "text/yaml",
        yml: "text/yaml",
        doc: "application/msword",
        docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        xls: "application/vnd.ms-excel",
        xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ppt: "application/vnd.ms-powerpoint",
        pptx: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        jpg: "image/jpeg",
        jpeg: "image/jpeg",
        png: "image/png",
        gif: "image/gif",
        webp: "image/webp",
        svg: "image/svg+xml",
        js: "text/javascript",
        ts: "application/typescript",
        html: "text/html",
        css: "text/css",
        zip: "application/zip"
      };
      return MIME_MAP[ext] || fallback;
    }

    // JSON helper
    function json(obj, status = 200) {
      return new Response(JSON.stringify(obj), {
        status,
        headers: { "content-type": "application/json" }
      });
    }

    // Helper: REST list documents for a store (used in /sync)
    async function rest_list_documents_for_store(file_search_store_name, apiKey) {
      const url = `https://generativelanguage.googleapis.com/v1beta/${file_search_store_name}/documents?key=${apiKey}`;
      try {
        const resp = await fetch(url, { method: "GET" });
        if (!resp.ok) return [];
        const data = await resp.json();
        return data.documents || [];
      } catch {
        return [];
      }
    }

    // Helper: poll operation until done (used in background). Returns opJson or null.
    async function pollOperationUntilDone(operationName, apiKey, timeoutMs = 25000, intervalMs = 2000) {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        try {
          const opResp = await fetch(`https://generativelanguage.googleapis.com/v1beta/${operationName}?key=${apiKey}`);
          if (!opResp.ok) {
            // non-200 — still keep trying until timeout
            await new Promise((r) => setTimeout(r, intervalMs));
            continue;
          }
          const opJson = await opResp.json();
          if (opJson.done) return opJson;
        } catch (e) {
          // ignore and retry until timeout
        }
        await new Promise((r) => setTimeout(r, intervalMs));
      }
      return null; // timed out
    }

    // ---------------------------
    // ROUTES
    // ---------------------------

    // CREATE STORE
    if (pathname === "/stores/create" && method === "POST") {
      try {
        const body = await request.json();
        const apiKey = body.api_key;
        const storeName = body.store_name;
        if (!apiKey || !storeName) return json({ error: "Missing api_key or store_name" }, 400);

        const ai = initAI(apiKey);
        const data = await load();
        if (data.file_stores[storeName]) return json({ error: "Store already exists" }, 400);

        const fsStore = await ai.fileSearchStores.create({ config: { displayName: storeName } });

        data.file_stores[storeName] = {
          store_name: storeName,
          file_search_store_name: fsStore.name,
          created_at: new Date().toISOString(),
          files: []
        };
        data.current_store_name = storeName;
        await save(data);

        return json({
          success: true,
          store_name: storeName,
          file_search_store_resource: fsStore.name,
          created_at: data.file_stores[storeName].created_at,
          file_count: 0
        });
      } catch (err) {
        return json({ error: err.toString() }, 500);
      }
    }

    // UPLOAD FILE — immediate response + background polling via ctx.waitUntil()
    if (pathname.startsWith("/stores/") && pathname.endsWith("/upload") && method === "POST") {
      const segments = pathname.split("/");
      const storeName = segments[2];

      const data = await load();
      const store = data.file_stores[storeName];
      if (!store) return json({ error: "Store not found" }, 404);

      const form = await request.formData();
      const apiKey = form.get("api_key");
      const files = form.getAll("files");
      if (!apiKey) return json({ error: "Missing api_key" }, 400);

      const fsStoreName = store.file_search_store_name;
      const results = [];

      // Prepare per-file metadata and kick background work using waitUntil
      for (const file of files) {
        const cleanedName = cleanFilename(file.name);
        const mimeType = detectMimeType(cleanedName);
        const arrayBuffer = await file.arrayBuffer();

        // Build FormData for manual upload
        const fd = new FormData();
        fd.append("file", new Blob([arrayBuffer], { type: mimeType }), cleanedName);
        // Gemini expects display_name in the metadata; include as form field
        fd.append("display_name", cleanedName);

        const uploadUrl = `https://generativelanguage.googleapis.com/v1beta/${fsStoreName}:uploadToFileSearchStore?key=${apiKey}`;

        let uploadedOk = false;
        let opName = null;

        try {
          const uploadResp = await fetch(uploadUrl, { method: "POST", body: fd });

          if (!uploadResp.ok) {
            const errText = await uploadResp.text();
            results.push({
              filename: cleanedName,
              uploaded: false,
              indexed: false,
              gemini_error: errText
            });
            continue;
          }

          const uploadJson = await uploadResp.json();

          // The upload returns an operation name we must poll
          opName = uploadJson?.name || null;

          // Immediately record an entry in KV with indexed=false and operationName
          const entry = {
            display_name: cleanedName,
            size_bytes: arrayBuffer.byteLength,
            uploaded_at: new Date().toISOString(),
            gemini_indexed: false,
            document_resource: null,
            document_id: null,
            gemini_error: null,
            operation_name: opName
          };
          store.files.push(entry);
          await save(data);

          uploadedOk = true;

        } catch (err) {
          results.push({
            filename: cleanedName,
            uploaded: false,
            indexed: false,
            gemini_error: err.toString()
          });
          continue;
        }

        // Kick background process to poll the operation and update KV when ready
        if (uploadedOk && opName) {
          ctx.waitUntil((async () => {
            try {
              // Poll operation up to 25s
              const opJson = await pollOperationUntilDone(opName, apiKey, 25000, 2000);
              if (opJson && opJson.done) {
                const docResource = opJson?.response?.fileSearchDocument?.name || null;
                const docId = docResource ? docResource.split("/").pop() : null;

                // Update KV entry: find the file by display_name AND operation_name
                const refreshData = await load();
                const filesArr = refreshData.file_stores?.[storeName]?.files || [];
                for (let f of filesArr) {
                  if (f.operation_name === opName && f.display_name === cleanedName) {
                    f.gemini_indexed = !!docResource;
                    f.document_resource = docResource;
                    f.document_id = docId;
                    f.gemini_error = f.gemini_error || null;
                    // remove operation_name now that done
                    delete f.operation_name;
                    break;
                  }
                }
                await save(refreshData);
              } else {
                // Didn't finish within timeframe — leave operation_name for /sync or future checks
                // No further action — indexing continues server-side at Gemini
              }
            } catch (bgErr) {
              // Log and swallow — we cannot fail the original response
              console.error("Background poll error:", bgErr);
            }
          })());
        }

        // Immediate response for this file (we do not wait for indexing)
        results.push({
          filename: cleanedName,
          uploaded: true,
          indexed: false,
          document_resource: null,
          document_id: null,
          gemini_error: null,
          operation_name: opName
        });
      } // for files

      // Return immediately — indexing happens in background
      return json({ success: true, results });
    }

    // LIST STORES
    if (pathname === "/stores" && method === "GET") {
      const apiKey = url.searchParams.get("api_key");
      if (!apiKey) return json({ error: "Missing api_key" }, 400);

      try {
        initAI(apiKey); // test key
      } catch (e) {
        return json({ error: e.toString() }, 400);
      }

      const data = await load();
      return json({ success: true, stores: Object.values(data.file_stores) });
    }

    // DELETE DOCUMENT
    if (pathname.startsWith("/stores/") && pathname.includes("/documents/") && method === "DELETE") {
      const segments = pathname.split("/");
      const storeName = segments[2];
      const documentId = segments[4];
      const apiKey = url.searchParams.get("api_key");
      if (!apiKey) return json({ error: "Missing api_key" }, 400);

      const data = await load();
      const store = data.file_stores[storeName];
      if (!store) return json({ error: "Store not found" }, 404);

      const fsStore = store.file_search_store_name;
      const deleteURL = `https://generativelanguage.googleapis.com/v1beta/${fsStore}/documents/${documentId}?force=true&key=${apiKey}`;

      const resp = await fetch(deleteURL, { method: "DELETE" });
      if (![200, 204].includes(resp.status)) {
        return json({ success: false, error: await resp.text() }, resp.status);
      }

      // remove from KV
      store.files = store.files.filter((f) => f.document_id !== documentId);
      await save(data);

      return json({ success: true, deleted_document_id: documentId });
    }

    // DELETE STORE
    if (pathname.startsWith("/stores/") && method === "DELETE") {
      const segments = pathname.split("/");
      const storeName = segments[2];
      const apiKey = url.searchParams.get("api_key");
      const data = await load();
      const store = data.file_stores[storeName];
      if (!store) return json({ error: "Store not found" }, 404);

      try {
        const ai = initAI(apiKey);
        await ai.fileSearchStores.delete({ name: store.file_search_store_name, config: { force: true }});
      } catch (_) {
        // swallow - still remove local metadata
      }

      delete data.file_stores[storeName];
      if (data.current_store_name === storeName) data.current_store_name = null;
      await save(data);

      return json({ success: true, deleted_store: storeName });
    }

    // ASK (RAG)
    if (pathname === "/ask" && method === "POST") {
      try {
        const body = await request.json();
        const apiKey = body.api_key;
        const stores = body.stores || [];
        const question = body.question;
        const systemPrompt = body.system_prompt;

        if (!apiKey) return json({ error: "Missing api_key" }, 400);
        if (!question) return json({ error: "Missing question" }, 400);

        const ai = initAI(apiKey);
        const data = await load();
        const fsStores = [];

        for (const s of stores) {
          if (data.file_stores?.[s]?.file_search_store_name) {
            fsStores.push(data.file_stores[s].file_search_store_name);
          }
        }
        if (fsStores.length === 0) return json({ error: "No valid File Search stores found for provided store names." }, 400);

        const response = await ai.models.generateContent({
          model: "gemini-2.5-flash",
          contents: question,
          config: {
            systemInstruction: systemPrompt || "You are a document assistant. Answer ONLY from the provided File Search stores.",
            tools: [
              { fileSearch: { fileSearchStoreNames: fsStores } }
            ]
          }
        });

        const grounding = response?.candidates?.[0]?.grounding_metadata || response?.candidates?.[0]?.groundingMetadata || null;
        return json({ success: true, response_text: response?.text || "", grounding_metadata: grounding });
      } catch (err) {
        return json({ error: err.toString() }, 500);
      }
    }

    // SYNC endpoint: fetch documents from Gemini and update KV for a store
    if (pathname.startsWith("/stores/") && pathname.endsWith("/sync") && method === "POST") {
      const segments = pathname.split("/");
      const storeName = segments[2];
      const body = await request.json();
      const apiKey = body?.api_key;
      if (!apiKey) return json({ error: "Missing api_key" }, 400);

      const data = await load();
      const store = data.file_stores[storeName];
      if (!store) return json({ error: "Store not found" }, 404);

      const fsStoreName = store.file_search_store_name;
      const docs = await rest_list_documents_for_store(fsStoreName, apiKey);

      // Match docs by displayName (or partial match) to our store.files to fill in missing ids
      let updated = 0;
      for (const d of docs) {
        const display = d.displayName || d.display_name || "";
        const name = d.name || "";
        if (!display) continue;

        // try to find matching entry in local metadata
        const localFile = store.files.find(f => f.display_name === display || (name && name.includes(f.display_name)));
        if (localFile && !localFile.document_id) {
          localFile.document_resource = name;
          localFile.document_id = name.split("/").pop();
          localFile.gemini_indexed = true;
          updated++;
        }
      }

      await save(data);
      return json({ success: true, updated_count: updated, total_remote_documents: docs.length });
    }

    return json({ error: "Route not found" }, 404);
  }
};
