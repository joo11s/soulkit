#!/usr/bin/env python3
"""
상품등록 자동화 파이프라인 — n8n 워크플로우 빌더
워크플로우 ID: Zn37i8WAmwonoMBX (해피비즈 프록시 웹훅 모음)

웹훅 엔드포인트: POST https://n8n.wonflowai.com/webhook/product-upload

입력 JSON 구조:
{
  "common": {
    "product_name", "consumer_price", "price", "supply_price",
    "stock", "display", "sale_status", "category_no", "tax_type",
    "options": [{"name": str, "values": [...]}]
  },
  "businesses": {
    "소울스토어": { "product_name", "images": {main, spec, detail, notice, extra}, "maker_info": {...} },
    "제이원투몰":  { ... },
    "행복산업":    { ... }
  }
}

파이프라인 흐름:
  1. 상품등록 Webhook (POST /product-upload)
  2. 상품등록 데이터 파싱 (Code)
  3. 병렬 처리:
     ├─ 소울스토어: 상품등록 → 이미지등록 → 옵션등록 → 소울스토어 결과
     ├─ 제이원투몰:  상품등록 → 이미지등록 → 옵션등록 → 제이원투몰 결과
     └─ 행복산업:    상품등록 → 이미지등록 → 옵션등록 → 행복산업 결과
  4. 결과 병합 (Merge)
  5. 결과 취합 (Code)
  6. HAPPYBIZ_DB 상품 저장 (Google Sheets: 1a8q2J6LXrzSf8BozovoABnGZ6brLEQPQvV5JCTPm3QI)
  7. 상품등록 텔레그램 알림 (chatId: 5060376494)

사업자별 Cafe24 credential:
  - 소울스토어: oAuth2Api KwRayi0rosYb4MS2 (soulstore3.cafe24api.com)
  - 제이원투몰:  oAuth2Api cpi6r9rl1fxGXkBU (jonetravel.cafe24api.com)
  - 행복산업:    oAuth2Api 2UYNWcHSyUG2xfPi (godqhrtksdjq.cafe24api.com)

사용법:
  export N8N_API_KEY=<key>
  python3 product_pipeline.py           # 파이프라인 빌드 및 PUT
  python3 product_pipeline.py --dry-run # JSON 생성만 (API 호출 없음)
"""
import json
import os
import sys
import urllib.request

N8N_BASE = "https://n8n.wonflowai.com/api/v1"
WORKFLOW_ID = "Zn37i8WAmwonoMBX"
PARSE_REF = "$('상품등록 데이터 파싱').first().json"

PARSE_CODE = """const body = $input.first().json.body || $input.first().json;
const common = body.common || {};
const businesses = body.businesses || {};
const domainMap = {
  '소울스토어': 'soulstore3.cafe24api.com',
  '제이원투몰': 'jonetravel.cafe24api.com',
  '행복산업':   'godqhrtksdjq.cafe24api.com'
};
const keyMap = { '소울스토어': 'soul', '제이원투몰': 'j1', '행복산업': 'happy' };
const result = {};
for (const [bizName, domain] of Object.entries(domainMap)) {
  const k = keyMap[bizName];
  const bizData = businesses[bizName] || {};
  const images = bizData.images || {};
  const taxType = bizData.tax_type || common.tax_type || 'A';
  const tax_calculation = taxType === 'B' ? 'B' : 'A';
  result[k] = {
    domain,
    product_name:    bizData.product_name || common.product_name || '',
    consumer_price:  common.consumer_price || 0,
    price:           common.price          || 0,
    supply_price:    common.supply_price   || 0,
    display:         common.display        || 'T',
    selling:         common.sale_status    || 'T',
    category_no:     parseInt(common.category_no) || 1,
    tax_calculation,
    description: [
      "<img src='" + (images.detail || '') + "' style='width:100%'>",
      "<img src='" + (images.notice || '') + "' style='width:100%'>",
      "<img src='" + (images.extra  || '') + "' style='width:100%'>"
    ].join(''),
    main_image:  images.main  || '',
    has_options: !!(common.options && common.options.length > 0),
    options:     common.options || []
  };
}
return [{ json: {
  product_name: common.product_name || '',
  soul:  result.soul,
  j1:    result.j1,
  happy: result.happy
} }];"""

COLLECT_CODE = """const items = $input.all();
const byBiz = {};
for (const item of items) {
  byBiz[item.json.business] = item.json.product_no;
}
const soulNo  = String(byBiz['소울스토어'] || '');
const j12No   = String(byBiz['제이원투몰']  || '');
const happyNo = String(byBiz['행복산업']    || '');
return [{ json: {
  task_id:          'prod_' + Date.now(),
  product_name:     $('상품등록 데이터 파싱').first().json.product_name,
  soul_product_no:  soulNo,
  j12_product_no:   j12No,
  happy_product_no: happyNo,
  status:           'success',
  created_at:       new Date().toISOString()
} }];"""


def expr(js):
    return "={{ " + js + " }}"


def cafe24_body(fields):
    parts = ['"' + k + '":' + v for k, v in fields.items()]
    return expr("JSON.stringify({" + ",".join(parts) + "})")


def headers_param():
    return {
        "sendHeaders": True,
        "headerParameters": {
            "parameters": [{"name": "X-Cafe24-Api-Version", "value": "2026-03-01"}]
        }
    }


def oauth_cred(label, cred_id):
    return {"oAuth2Api": {"id": cred_id, "name": label + " oAuth2Api"}}


def build_nodes():
    """Build all 18 new pipeline nodes."""
    Y0, Y_SOUL, Y_J12, Y_HAPPY = 2280, 2100, 2280, 2460
    X = dict(wh=-200, parse=100, reg=420, img=700, opt=980, set=1260,
             merge=1540, collect=1820, sheets=2100, tg=2380)

    nodes = []

    # 1. Webhook
    nodes.append({
        "parameters": {"httpMethod": "POST", "path": "product-upload", "options": {}},
        "id": "prod-webhook", "name": "상품등록 Webhook",
        "type": "n8n-nodes-base.webhook", "typeVersion": 2,
        "position": [X["wh"], Y0], "webhookId": "product-upload-v1"
    })

    # 2. Parse Code
    nodes.append({
        "parameters": {"jsCode": PARSE_CODE},
        "id": "prod-parse", "name": "상품등록 데이터 파싱",
        "type": "n8n-nodes-base.code", "typeVersion": 2, "position": [X["parse"], Y0]
    })

    # 3–14. Business nodes (3 × 4)
    businesses = [
        ("소울스토어", "soul",  "soulstore3.cafe24api.com",    "KwRayi0rosYb4MS2", Y_SOUL),
        ("제이원투몰",  "j1",    "jonetravel.cafe24api.com",   "cpi6r9rl1fxGXkBU", Y_J12),
        ("행복산업",   "happy", "godqhrtksdjq.cafe24api.com", "2UYNWcHSyUG2xfPi", Y_HAPPY),
    ]

    for label, key, domain, cred_id, y in businesses:
        pr = PARSE_REF + "." + key
        reg_name = label + " 상품등록"
        nid_prefix = key

        # 상품등록
        nodes.append({
            "parameters": {
                "authentication": "genericCredentialType", "genericAuthType": "oAuth2Api",
                "method": "POST", "url": "https://" + domain + "/api/v2/admin/products",
                **headers_param(), "sendBody": True, "specifyBody": "json",
                "jsonBody": cafe24_body({
                    "shop_no": "1",
                    "product_name":    pr + ".product_name",
                    "price":           pr + ".price",
                    "supply_price":    pr + ".supply_price",
                    "consumer_price":  pr + ".consumer_price",
                    "display":         pr + ".display",
                    "selling":         pr + ".selling",
                    "category":        '[{"category_no":' + pr + ".category_no}]",
                    "description":     pr + ".description",
                    "tax_calculation": pr + ".tax_calculation",
                }),
                "options": {}
            },
            "credentials": oauth_cred(label, cred_id),
            "id": nid_prefix + "-reg", "name": reg_name,
            "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2, "position": [X["reg"], y]
        })

        # 이미지등록
        url_img = expr('"https://' + domain + '/api/v2/products/" + $("' + reg_name + '").first().json.product.product_no + "/images"')
        nodes.append({
            "parameters": {
                "authentication": "genericCredentialType", "genericAuthType": "oAuth2Api",
                "method": "POST", "url": url_img,
                **headers_param(), "sendBody": True, "specifyBody": "json",
                "jsonBody": cafe24_body({
                    "image_upload_type": '"U"',
                    "detail_image": pr + ".main_image",
                    "list_image":   pr + ".main_image",
                    "tiny_image":   pr + ".main_image",
                    "small_image":  pr + ".main_image",
                }),
                "options": {}
            },
            "credentials": oauth_cred(label, cred_id),
            "id": nid_prefix + "-img", "name": label + " 이미지등록",
            "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2, "position": [X["img"], y]
        })

        # 옵션등록
        url_opt = expr('"https://' + domain + '/api/v2/products/" + $("' + reg_name + '").first().json.product.product_no + "/options"')
        nodes.append({
            "parameters": {
                "authentication": "genericCredentialType", "genericAuthType": "oAuth2Api",
                "method": "POST", "url": url_opt,
                **headers_param(), "sendBody": True, "specifyBody": "json",
                "jsonBody": cafe24_body({
                    "has_option":  pr + ".has_options ? 'T' : 'F'",
                    "option_type": '"S"',
                    "options":     pr + ".options",
                }),
                "options": {}
            },
            "credentials": oauth_cred(label, cred_id),
            "id": nid_prefix + "-opt", "name": label + " 옵션등록",
            "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2, "position": [X["opt"], y]
        })

        # 결과 Set
        nodes.append({
            "parameters": {
                "assignments": {"assignments": [
                    {"id": nid_prefix + "-b", "name": "business",   "value": label,                                                    "type": "string"},
                    {"id": nid_prefix + "-n", "name": "product_no", "value": expr('$("' + reg_name + '").first().json.product.product_no'), "type": "number"},
                ]},
                "options": {}
            },
            "id": nid_prefix + "-set", "name": label + " 결과",
            "type": "n8n-nodes-base.set", "typeVersion": 3.4, "position": [X["set"], y]
        })

    # 15. Merge
    nodes.append({
        "parameters": {"mode": "append", "options": {}},
        "id": "prod-merge", "name": "결과 병합",
        "type": "n8n-nodes-base.merge", "typeVersion": 3, "position": [X["merge"], Y_J12]
    })

    # 16. Collect Code
    nodes.append({
        "parameters": {"jsCode": COLLECT_CODE},
        "id": "prod-collect", "name": "결과 취합",
        "type": "n8n-nodes-base.code", "typeVersion": 2, "position": [X["collect"], Y_J12]
    })

    # 17. Google Sheets
    nodes.append({
        "parameters": {
            "operation": "append",
            "documentId": {"__rl": True, "value": "1a8q2J6LXrzSf8BozovoABnGZ6brLEQPQvV5JCTPm3QI", "mode": "id"},
            "sheetName":  {"__rl": True, "value": "products", "mode": "name"},
            "columns": {
                "mappingMode": "defineBelow",
                "value": {
                    "task_id":              "={{ $json.task_id }}",
                    "product_name":         "={{ $json.product_name }}",
                    "소울스토어_product_no": "={{ $json.soul_product_no }}",
                    "제이원투몰_product_no":  "={{ $json.j12_product_no }}",
                    "행복산업_product_no":    "={{ $json.happy_product_no }}",
                    "status":               "={{ $json.status }}",
                    "created_at":           "={{ $json.created_at }}"
                },
                "matchingColumns": [], "schema": []
            },
            "options": {}
        },
        "credentials": {"googleSheetsOAuth2Api": {"id": "IKkQrOQLB0JjUmfP", "name": "Google Sheets account 5"}},
        "id": "prod-sheets", "name": "HAPPYBIZ_DB 상품 저장",
        "type": "n8n-nodes-base.googleSheets", "typeVersion": 4.5, "position": [X["sheets"], Y_J12]
    })

    # 18. Telegram
    nodes.append({
        "parameters": {
            "chatId": "5060376494",
            "text": ("=✅ 상품등록 완료\n"
                     "상품명: {{ $json.product_name }}\n"
                     "소울스토어: {{ $json.soul_product_no }}\n"
                     "제이원투몰: {{ $json.j12_product_no }}\n"
                     "행복산업: {{ $json.happy_product_no }}"),
            "additionalFields": {}
        },
        "credentials": {"telegramApi": {"id": "jIVQPq2Y7scGUtaw", "name": "Telegram account"}},
        "id": "prod-telegram", "name": "상품등록 텔레그램 알림",
        "type": "n8n-nodes-base.telegram", "typeVersion": 1.2, "position": [X["tg"], Y_J12]
    })

    return nodes


def build_connections():
    def c(node, idx=0):
        return {"node": node, "type": "main", "index": idx}

    return {
        "상품등록 Webhook":      {"main": [[c("상품등록 데이터 파싱")]]},
        "상품등록 데이터 파싱":  {"main": [[c("소울스토어 상품등록"), c("제이원투몰 상품등록"), c("행복산업 상품등록")]]},
        "소울스토어 상품등록":   {"main": [[c("소울스토어 이미지등록")]]},
        "소울스토어 이미지등록": {"main": [[c("소울스토어 옵션등록")]]},
        "소울스토어 옵션등록":   {"main": [[c("소울스토어 결과")]]},
        "소울스토어 결과":       {"main": [[c("결과 병합", 0)]]},
        "제이원투몰 상품등록":   {"main": [[c("제이원투몰 이미지등록")]]},
        "제이원투몰 이미지등록": {"main": [[c("제이원투몰 옵션등록")]]},
        "제이원투몰 옵션등록":   {"main": [[c("제이원투몰 결과")]]},
        "제이원투몰 결과":       {"main": [[c("결과 병합", 1)]]},
        "행복산업 상품등록":     {"main": [[c("행복산업 이미지등록")]]},
        "행복산업 이미지등록":   {"main": [[c("행복산업 옵션등록")]]},
        "행복산업 옵션등록":     {"main": [[c("행복산업 결과")]]},
        "행복산업 결과":         {"main": [[c("결과 병합", 2)]]},
        "결과 병합":             {"main": [[c("결과 취합")]]},
        "결과 취합":             {"main": [[c("HAPPYBIZ_DB 상품 저장")]]},
        "HAPPYBIZ_DB 상품 저장": {"main": [[c("상품등록 텔레그램 알림")]]},
    }


def get_workflow(api_key):
    req = urllib.request.Request(
        f"{N8N_BASE}/workflows/{WORKFLOW_ID}",
        headers={"X-N8N-API-KEY": api_key}
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def put_workflow(api_key, body):
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"{N8N_BASE}/workflows/{WORKFLOW_ID}",
        data=data,
        method="PUT",
        headers={
            "X-N8N-API-KEY": api_key,
            "Content-Type": "application/json"
        }
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def main():
    dry_run = "--dry-run" in sys.argv

    api_key = os.environ.get("N8N_API_KEY", "")
    if not api_key and not dry_run:
        key_file = os.path.expanduser("~/.n8n_api_key")
        if os.path.exists(key_file):
            api_key = open(key_file).read().strip()
    if not api_key and not dry_run:
        print("ERROR: N8N_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    print("Fetching current workflow...")
    wf = get_workflow(api_key) if not dry_run else {"name": "DRY-RUN", "nodes": [], "connections": {}, "settings": {}, "staticData": None}

    new_nodes = build_nodes()
    new_conns = build_connections()

    wf["nodes"] = wf.get("nodes", []) + new_nodes
    wf["connections"] = {**wf.get("connections", {}), **new_conns}

    put_body = {k: wf[k] for k in ["name", "nodes", "connections", "settings", "staticData"] if k in wf}

    if dry_run:
        out = "product_pipeline_dry.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(put_body, f, ensure_ascii=False, indent=2)
        print(f"Dry-run: saved to {out}")
        print(f"Would add {len(new_nodes)} nodes")
        return

    print(f"Adding {len(new_nodes)} nodes to workflow {WORKFLOW_ID}...")
    result = put_workflow(api_key, put_body)
    print(f"✅ Updated: {result['name']} — {len(result['nodes'])} nodes total")
    print(f"   updatedAt: {result.get('updatedAt')}")


if __name__ == "__main__":
    main()
