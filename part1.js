// part1.js
const fs=require("fs");
const path=require("path");
const https=require("https");
//API request settings
const LIMIT=100;//fetch 100 records per request
const CONCURRENCY_LIMIT=Number(process.env.CONCURRENCY_LIMIT||3);//3 parallel workers
const API_KEY="WiHMbuutQVJAveess01nfvW3EuGXSHPgE7HnXqEK";//API key 

//date range
const START="20000101";//first date with data  within range
const END="20250930";//last date with data within range

const BASE="https://api.fda.gov/food/event.json"; //base endpoint
const DATA_DIR=path.join(__dirname,"data");//create ./data folder if it doesn't already exist

if(!fs.existsSync(DATA_DIR))fs.mkdirSync(DATA_DIR,{recursive:true});

function sleep(ms){return new Promise(r=>setTimeout(r,ms));}//function for pacing of request

//change YYYYMMDD string to UTC time
function toDate(ymd){
  return new Date(Date.UTC(
    Number(ymd.slice(0,4)),
    Number(ymd.slice(4,6))-1,
    Number(ymd.slice(6,8))
  ));
}
//change date object to YYYYMMDD string
function toYmd(d){
  return d.getUTCFullYear()
    +String(d.getUTCMonth()+1).padStart(2,"0")
    +String(d.getUTCDate()).padStart(2,"0");
}
//get last day of the month
function endOfMonth(d){
  return new Date(Date.UTC(d.getUTCFullYear(),d.getUTCMonth()+1,0));
}
//move date forward by one day
function nextDay(d){
  const x=new Date(d);
  x.setUTCDate(x.getUTCDate()+1);
  return x;
}
//makes buckets for parallel work
function makeBuckets(){
  const out=[];
  let cur=toDate(START);
  const end=toDate(END);
  while(cur<=end){
    const mEnd=endOfMonth(cur);
    out.push([toYmd(cur),toYmd(mEnd<=end?mEnd:end)]);
    cur=nextDay(mEnd);
  }
  return out;
}
//builds first API URL for month range
function firstUrl(a,b){
  const u=new URL(BASE);
  u.searchParams.set("api_key",API_KEY);
  u.searchParams.set("search",`date_started:[${a} TO ${b}]`);
  u.searchParams.set("limit",LIMIT);
  u.searchParams.set("sort","date_started:asc");
  return u.toString();
}

//get next URL from link
function nextUrlFromLink(link){
  if(!link)return null;
  const parts=link.split(",");
  for(const p of parts){
    if(p.includes('rel="next"')){
      const m=p.match(/<([^>]+)>/);
      if(m)return m[1];
    }
  }
  return null;
}
//does HTTPS request, retries on rate limit: 429
function fetchJson(url,attempt=1){
  return new Promise((resolve,reject)=>{
    const req=https.get(url,{headers:{"User-Agent":"part1-downloader"}},res=>{
      let body="";
      res.setEncoding("utf8");
      res.on("data",c=>body+=c);
      res.on("end",async()=>{
        const code=res.statusCode||0;

        if(code===429||code>=500){
          const retryAfter=Number(res.headers["retry-after"]||0);
          const waitMs=retryAfter?retryAfter*1000:Math.min(30000,500*Math.pow(2,attempt-1));
          console.log(`retry ${code} in ${waitMs}ms`);
          await sleep(waitMs);
          return resolve(fetchJson(url,attempt+1));
        }

        if(code<200||code>=300){
          return reject(new Error(`HTTP ${code}: ${body.slice(0,160)}`));
        }

        try{
          resolve({json:JSON.parse(body),headers:res.headers});
        }catch(e){
          reject(new Error("bad JSON"));
        }
      });
    });

    req.on("error",reject);
    req.setTimeout(30000,()=>req.destroy(new Error("timeout")));
  });
}
//downloads pages for bucket, saves each page to JSON file
async function downloadBucket(a,b){
  let url=firstUrl(a,b);
  let page=0;
  let records=0;

  while(url){
    page++;
    const {json,headers}=await fetchJson(url);

    const results=Array.isArray(json.results)?json.results:[];
    records+=results.length;

    const file=`food_event_${a}_${b}_p${(page)}.json`;
    fs.writeFileSync(path.join(DATA_DIR,file),JSON.stringify(json));

    url=nextUrlFromLink(headers.link||headers.Link);

    if(page%25===0)console.log(`${a}..${b} pages=${page} records=${records}`);
    await sleep(30);
  }

  return records;
}

async function main(){
  const buckets=makeBuckets();
  console.log(`months: ${buckets.length}, concurrency: ${CONCURRENCY_LIMIT}, limit: ${LIMIT}`);
  let index=0;
  let total=0;
  async function worker(id){
    while(true){
      const i=index++;
      if(i>=buckets.length)return;
      const [a,b]=buckets[i];
      console.log(`[w${id}] ${a}..${b}`);
      const count=await downloadBucket(a,b);
      total+=count;
      console.log(`[w${id}] done ${a}..${b} records=${count}`);
    }
  }

  const workers=[];
  for(let i=1;i<=CONCURRENCY_LIMIT;i++)workers.push(worker(i));
  await Promise.all(workers);

  console.log(`finished total=${total}`);
}
//error catching
main().catch(err=>{
  console.error("fatal:",err.message);
  process.exitCode=1;
});
