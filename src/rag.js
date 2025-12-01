import { GoogleGenAI } from "@google/genai";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname;
    const method = request.method.toUpperCase();

    // Init Gemini client
    const initAI = (apiKey) => new GoogleGenAI({ apiKey });

    // Load + save KV
    async function load() {
      const raw = await env.RAG.get("stores");
      return raw ? JSON.parse(raw) : { file_stores: {}, current_store_name: null };
    }
    async function save(data) {
      await env.RAG.put("stores", JSON.stringify(data));
    }

    // Clean filenames
    const cleanFilename = (name) =>
      name.trim().replace(/\s+/g, "_").replace(/[^A-Za-z0-9_\-\.]/g, "_").substring(0, 180);

    // Mime detection based on extension
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

    // ======================================================
    // CREATE STORE
    // ======================================================
    if (pathname === "/stores/create" && method === "POST") {
      const body = await request.json();
      const apiKey = body.api_key;
      const storeName = body.store_name;

      if (!apiKey || !storeName) return json({ error: "Missing api_key or store_name" }, 400);

      const ai = initAI(apiKey);
      const data = await load();

      if (data.file_stores[storeName])
        return json({ error: "Store already exists" }, 400);

      try {
        const fsStore = await ai.fileSearchStores.create({
          config: { displayName: storeName }
        });

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
      } catch (e) {
        return json({ error: e.toString() }, 500);
      }
    }

    // ======================================================
    // UPLOAD FILE
    // ======================================================
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

      const ai = initAI(apiKey);
      const fsStoreName = store.file_search_store_name;

      const results = [];

      for (const file of files) {
        const cleanedName = cleanFilename(file.name);
        const mimeType = detectMimeType(cleanedName);
        const arrayBuffer = await file.arrayBuffer();

        console.log("UPLOAD:", cleanedName, "MIME:", mimeType);

        let operation;
        let docResource = null;
        let docId = null;

        try {
          operation = await ai.fileSearchStores.uploadToFileSearchStore({
            file: {
              buffer: arrayBuffer,
              displayName: cleanedName
            },
            fileSearchStoreName: fsStoreName,
            config: {
              displayName: cleanedName,
              mimeType: mimeType      // << THE CRITICAL FIX
            }
          });

          // poll LRO
          while (!operation.done) {
            await new Promise((res) => setTimeout(res, 2000));
            operation = await ai.operations.get({ operation });
          }

          docResource = operation?.response?.fileSearchDocument?.name || null;
          if (docResource) docId = docResource.split("/").pop();

        } catch (err) {
          results.push({
            filename: cleanedName,
            uploaded: false,
            indexed: false,
            gemini_error: err.toString()
          });
          continue;
        }

        // Save metadata
        store.files.push({
          display_name: cleanedName,
          size_bytes: arrayBuffer.byteLength,
          uploaded_at: new Date().toISOString(),
          gemini_indexed: true,
          document_resource: docResource,
          document_id: docId,
          gemini_error: null
        });

        await save(data);

        results.push({
          filename: cleanedName,
          uploaded: true,
          indexed: true,
          document_resource: docResource,
          document_id: docId,
          gemini_error: null
        });
      }

      return json({ success: true, results });
    }

    // ======================================================
    // LIST STORES
    // ======================================================
    if (pathname === "/stores" && method === "GET") {
      const apiKey = url.searchParams.get("api_key");
      if (!apiKey) return json({ error: "Missing api_key" }, 400);

      try {
        initAI(apiKey);
      } catch (e) {
        return json({ error: e.toString() }, 400);
      }

      const data = await load();
      return json({ success: true, stores: Object.values(data.file_stores) });
    }

    // ======================================================
    // DELETE DOCUMENT
    // ======================================================
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

      const deleteURL =
        `https://generativelanguage.googleapis.com/v1beta/${fsStore}/documents/${documentId}?force=true&key=${apiKey}`;

      const resp = await fetch(deleteURL, { method: "DELETE" });

      if (![200, 204].includes(resp.status)) {
        return json({ success: false, error: await resp.text() }, resp.status);
      }

      store.files = store.files.filter((f) => f.document_id !== documentId);
      await save(data);

      return json({ success: true, deleted_document_id: documentId });
    }

    // ======================================================
    // DELETE STORE
    // ======================================================
    if (pathname.startsWith("/stores/") && method === "DELETE") {
      const segments = pathname.split("/");
      const storeName = segments[2];
      const apiKey = url.searchParams.get("api_key");

      const data = await load();
      const store = data.file_stores[storeName];

      if (!store) return json({ error: "Store not found" }, 404);

      try {
        const ai = initAI(apiKey);
        await ai.fileSearchStores.delete({
          name: store.file_search_store_name,
          config: { force: true }
        });
      } catch (_) {}

      delete data.file_stores[storeName];
      if (data.current_store_name === storeName) data.current_store_name = null;

      await save(data);

      return json({ success: true, deleted_store: storeName });
    }

    // ======================================================
    // ASK (RAG)
    // ======================================================
    if (pathname === "/ask" && method === "POST") {
      const body = await request.json();
      const apiKey = body.api_key;
      const stores = body.stores;
      const question = body.question;
      const systemPrompt = body.system_prompt;

      const ai = initAI(apiKey);
      const data = await load();
      const fsStores = [];

      for (const s of stores) {
        if (data.file_stores[s]) {
          fsStores.push(data.file_stores[s].file_search_store_name);
        }
      }

      if (fsStores.length === 0)
        return json({ error: "No valid stores found" }, 400);

      try {
        const response = await ai.models.generateContent({
          model: "gemini-2.5-flash",
          contents: question,
          config: {
            systemInstruction:
              systemPrompt ||
              "Answer ONLY based on provided File Search store documents.",
            tools: [
              {
                fileSearch: {
                  fileSearchStoreNames: fsStores
                }
              }
            ]
          }
        });

        return json({
          success: true,
          response_text: response.text,
          grounding_metadata: response.candidates?.[0]?.groundingMetadata || null
        });
      } catch (e) {
        return json({ error: e.toString() }, 500);
      }
    }

    return json({ error: "Route not found" }, 404);
  }
};

// json helper
function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json" }
  });
}
