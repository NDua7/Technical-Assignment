#part2.py

#import statements
import os
import sys
import json
import re
from datetime import datetime
from collections import Counter, defaultdict
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

#folders for input date, output charts
DATA_DIR="data"
CHART_DIR="charts"

#parses command line arguments
# no years -> 2000 to present
# 1 year -> year to present
# 2 years -> inclusive range
# other words are product filter
def parse_args(argv):
  years=[]
  words=[]
  for t in argv:
    if re.fullmatch(r"\d{4}",t.strip()):
      years.append(int(t))
    else:
      words.append(t)
  years=years[:2]
  product=" ".join(words).strip() if words else None

  if len(years)==0:
    return 2000,None,product
  if len(years)==1:
    return years[0],None,product
  a=min(years[0],years[1])
  b=max(years[0],years[1])
  return a,b,product
#clean text handles casing, special characters, extra spaces, and different spellings
def clean_text(s):
  s=(s or "").strip().upper()
  if not s:
    return ""
  s=s.replace("&"," AND ")
  s=re.sub(r"[^A-Z0-9]+"," ",s)
  s=re.sub(r"\s+"," ",s).strip()
  return s

# common words removed from product names
STOP={"THE","AND","WITH","FOR","OF","TO","IN","A","AN","OR"}
FORM={"TABLET","TABLETS","CAPSULE","CAPSULES","SOFTGEL","SOFTGELS","GUMMY","GUMMIES",
      "CHEW","CHEWABLE","CHEWABLES","POWDER","LIQUID","DROPS","SPRAY","GEL","LOTION",
      "CREAM","DRINK","BEVERAGE","BAR","BARS","SHAKE","SYRUP","TEA","PACKET","BOTTLE","JAR"}
UNITS={"MG","MCG","G","GRAM","GRAMS","ML","L","IU","OZ","LB","%","PERCENT","CFU"}
# normalize product names to reduce repeated names, removes dosage info and packaging words
def norm_product(s):
  s=clean_text(s)
  if not s:
    return ""

  # remove super common noise phrases
  s=re.sub(r"\bNO\s+UPC\b","",s)
  s=re.sub(r"\bUPC\b","",s)

  # remove dosage patterns like "500 MG", "0.5 MG", "1000IU", etc.
  s=re.sub(r"\b\d+(\.\d+)?\s*(MG|MCG|G|GRAM|GRAMS|ML|L|IU|OZ|LB|CFU)\b","",s)
  s=re.sub(r"\b\d+(\.\d+)?\b","",s)

  # remove packaging/form words (you already have FORM/UNITS)
  parts=[]
  for w in s.split():
    if w in STOP or w in FORM or w in UNITS:
      continue
    parts.append(w)
  s=" ".join(parts)
  s=re.sub(r"\s+"," ",s).strip()
  if not s:
    return ""

  # aggressive-ish: if product name is long, keep first 1-2 "core" tokens
  # (this helps drop descriptors like REGULAR/RAPID/RELEASE/etc.)
  words=s.split()
  if len(words)>=4:
    return " ".join(words[:2])
  if len(words)==3:
    return " ".join(words[:2])
  return s

##normalize reaction and outcome wordings
def norm_outcome(s):
  # keep outcomes less aggressive so you don't accidentally merge categories
  return clean_text(s)

def norm_reaction(s):
  s=clean_text(s)
  if not s:
    return ""

  # remove staging / location / severity details
  s=re.sub(r"\b(NOS|STAGE|GRADE|TYPE|SEVERE|MILD|MODERATE|ACUTE|CHRONIC)\b","",s)
  s=re.sub(r"\b(UPPER|LOWER|LEFT|RIGHT|BILATERAL|GENERALIZED|LOCALIZED)\b","",s)

  # remove roman numerals and digits (stage I/II/III, etc.)
  s=re.sub(r"\b[IVX]{1,6}\b","",s)
  s=re.sub(r"\b\d+\b","",s)

  s=re.sub(r"\s+"," ",s).strip()
  if not s:
    return ""

  # aggressive merge: keep first 2 words if long
  words=s.split()
  if len(words)>=3:
    return " ".join(words[:2])
  return s

#get year from date_started, returns none if invalid
def year_from_record(r):
  ds=r.get("date_started") or r.get("date_created") or ""
  if isinstance(ds,str) and len(ds)>=4 and ds[:4].isdigit():
    return int(ds[:4])
  return None
#get product names from record
def get_products(r):
  prods=r.get("products")
  if not isinstance(prods,list):
    return []
  out=[]
  for p in prods:
    if not isinstance(p,dict):
      continue
    name=p.get("name_brand") or p.get("name") or p.get("brand_name") or ""
    role=str(p.get("role") or "").upper()
    if name:
      out.append((name,role))
  return out
#get reactions from record
def get_reactions(r):
  rx=r.get("reactions")
  if not isinstance(rx,list):
    return []
  out=[]
  for x in rx:
    if isinstance(x,dict):
      v=x.get("reactionmeddrapt") or x.get("reaction") or x.get("term")
    else:
      v=x
    if v:
      out.append(str(v))
  return out
#get outcomes from record
def get_outcomes(r):
  oc=r.get("outcomes")
  if not isinstance(oc,list):
    return []
  out=[]
  for x in oc:
    if isinstance(x,dict):
      v=x.get("outcome") or x.get("term")
    else:
      v=x
    if v:
      out.append(str(v))
  return out
#get gender if available from record
def get_gender(r):
  c=r.get("consumer") or {}
  g=c.get("gender") or r.get("gender") or r.get("patient",{}).get("sex")
  if not g:
    return None
  g=str(g).strip().upper()
  if g in {"F","FEMALE"}:
    return "F"
  if g in {"M","MALE"}:
    return "M"
  return None
#converts age to years, filters unrealistic ages, handles units of years/months/days
def get_age_years(r):
  c=r.get("consumer") or {}
  age=c.get("age") or c.get("age_years") or r.get("age") or r.get("patient",{}).get("age")
  unit=c.get("age_unit") or c.get("age_unit_code") or r.get("age_unit") or r.get("patient",{}).get("age_unit")
  if age is None:
    return None
  try:
    a=float(age)
  except:
    return None
  if a<=0 or a>1200:
    return None
  u=(str(unit).strip().upper() if unit is not None else "Y")
  if u in {"Y","YR","YRS","YEAR","YEARS","801"}:
    y=a
  elif u in {"M","MO","MOS","MONTH","MONTHS","802"}:
    y=a/12.0
  elif u in {"D","DAY","DAYS","803"}:
    y=a/365.0
  else:
    y=a
  if y<=0 or y>120:
    return None
  return y
#tracks most common original spelling for display, adds item to counter
def add_count(counter,rep_map,raw,norm_fn):
  raw=(raw or "").strip()
  if not raw:
    return
  n=norm_fn(raw)
  if not n:
    return
  counter[n]+=1
  rep_map[n][raw]+=1
#returns top 25 items, uses most common spelling for output
def top25(counter,rep_map):
  out=[]
  for n,ct in counter.most_common(25):
    # show the most common original string for that normalized bucket
    label=rep_map[n].most_common(1)[0][0] if n in rep_map and rep_map[n] else n
    out.append((label,ct))
  return out
#iterate through data, apply filters, get statistics
def main():
  start_year,end_year,product_filter=parse_args(sys.argv[1:])
  pf=product_filter.upper() if product_filter else None

  if not os.path.isdir(DATA_DIR):
    print("missing ./data folder")
    sys.exit(1)

  files=[os.path.join(DATA_DIR,f) for f in os.listdir(DATA_DIR) if f.lower().endswith(".json")]
  files.sort()

  total=0
  by_year=Counter()

  outcomes=Counter()
  reactions=Counter()
  products=Counter()

  outcomes_rep=defaultdict(Counter)
  reactions_rep=defaultdict(Counter)
  products_rep=defaultdict(Counter)

  ages_all=[]
  ages_f=[]
  ages_m=[]
  ages_by_year=defaultdict(list)

  max_seen_year=None

  for fp in files:
    try:
      obj=json.load(open(fp,"r",encoding="utf-8"))
    except:
      continue
    rows=obj.get("results")
    if not isinstance(rows,list):
      continue

    for r in rows:
      if not isinstance(r,dict):
        continue

      y=year_from_record(r)
      if y is None:
        continue
      if max_seen_year is None or y>max_seen_year:
        max_seen_year=y

      if y<start_year:
        continue
      if end_year is not None and y>end_year:
        continue

      # product filter (must match suspect product substring)
      prods=get_products(r)
      if pf:
        ok=False
        for name,role in prods:
          roleu=(role or "").upper()
          suspect=("SUSPECT" in roleu) or (roleu=="")
          if suspect and pf in name.upper():
            ok=True
            break
        if not ok:
          continue

      total+=1
      by_year[y]+=1

      for o in get_outcomes(r):
        add_count(outcomes,outcomes_rep,o,norm_outcome)
      for rx in get_reactions(r):
        add_count(reactions,reactions_rep,rx,norm_reaction)

      for name,role in prods:
        roleu=(role or "").upper()
        if roleu and "SUSPECT" not in roleu:
          continue
        add_count(products,products_rep,name,norm_product)

      age=get_age_years(r)
      if age is not None:
        ages_all.append(age)
        ages_by_year[y].append(age)
        g=get_gender(r)
        if g=="F":
          ages_f.append(age)
        elif g=="M":
          ages_m.append(age)

  if max_seen_year is None:
    max_seen_year=start_year
  if end_year is None:
    end_year=max_seen_year

  print(f"Total Records matching the criteria: {total}")

  print("\nTop 25 Outcomes:")
  for name,ct in top25(outcomes,outcomes_rep):
    print(f"{ct}\t{name}")

  print("\nTop 25 Reactions:")
  for name,ct in top25(reactions,reactions_rep):
    print(f"{ct}\t{name}")

  print("\nTop 25 Suspect Products:")
  for name,ct in top25(products,products_rep):
    print(f"{ct}\t{name}")

  def avg(x):
    if not x:
      return "N/A"
    return f"{float(np.mean(np.array(x,dtype=float))):.2f}"

  print("\nAverage Consumer Age (years):")
  print(f"Total Avg:\t{avg(ages_all)}")
  print(f"Female Avg:\t{avg(ages_f)}")
  print(f"Male Avg:\t{avg(ages_m)}")

  os.makedirs(CHART_DIR,exist_ok=True)
  ts=datetime.now().strftime("%Y%m%d_%H%M%S")
  out_path=os.path.join(CHART_DIR,f"{ts}.png")

  # use pandas a bit (requirement) but keep it light
  years=list(range(start_year,end_year+1))
  counts=[by_year.get(y,0) for y in years]
  df_counts=pd.DataFrame({"year":years,"cases":counts})

  fig=plt.figure(figsize=(14,6))

  ax1=fig.add_subplot(1,2,1)
  ax1.bar(df_counts["year"],df_counts["cases"])
  ax1.set_title("Total cases by Year")
  ax1.set_xlabel("Year")
  ax1.set_ylabel("Cases")
  ax1.tick_params(axis="x",rotation=45)

  ax2=fig.add_subplot(1,2,2)
  if not ages_all:
    ax2.text(0.5,0.5,"No age data available",ha="center",va="center")
    ax2.set_axis_off()
  else:
    bins=np.arange(-0.5,120.5,1)
    yrs_with_ages=sorted([y for y in years if y in ages_by_year and len(ages_by_year[y])>0])
    for y in yrs_with_ages:
      ax2.hist(ages_by_year[y],bins=bins,alpha=0.18,label=str(y))
    ax2.set_title("Distribution of consumer ages (by year)")
    ax2.set_xlabel("Age (years)")
    ax2.set_ylabel("Count")
    if len(yrs_with_ages)<=12:
      ax2.legend(fontsize=8,frameon=False)

  plt.tight_layout()
  plt.savefig(out_path,dpi=150)
  plt.close(fig)

  print(f"\nSaved chart: {out_path}")

if __name__=="__main__":
  main()
