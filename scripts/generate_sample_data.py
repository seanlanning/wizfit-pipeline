import csv, random, datetime as dt
random.seed(42)
start=dt.date(2026,6,1); weeks=12
goals=["fat_loss","fat_loss","fat_loss","recomp","muscle_gain"]
progs=["Foundation","Discipline 90","Feel Alive Elite"]
clients=[]
for i in range(1,16):
    cid=f"WF-{i:03d}"; g=random.choice(goals)
    sw=round(random.uniform(165,270),1)
    gw=round(sw-random.uniform(20,50),1) if g=="fat_loss" else round(sw+random.uniform(-8,12),1)
    sd=start+dt.timedelta(days=random.randint(0,10))
    clients.append(dict(client_id=cid,start_date=sd.isoformat(),goal=g,program=random.choice(progs),start_weight_lbs=sw,goal_weight_lbs=gw,adherence=random.uniform(0.55,0.97)))
def messy_id(c): 
    r=random.random()
    return f" {c.lower()} " if r<0.05 else (c.lower() if r<0.1 else c)
def messy_date(d):
    return d.strftime("%m/%d/%Y") if random.random()<0.2 else d.isoformat()
ck=[];wo=[];nu=[]
exercises=["Back Squat","Bench Press","Deadlift","Overhead Press","Barbell Row","Pull-Up"]
for c in clients:
    sd=dt.date.fromisoformat(c["start_date"]); w=c["start_weight_lbs"]
    rate={"fat_loss":-1.4,"recomp":-0.3,"muscle_gain":0.4}[c["goal"]]
    base={e:random.randint(95,315) for e in exercises}
    for wk in range(weeks):
        d=sd+dt.timedelta(weeks=wk)
        if random.random()<c["adherence"]:
            w=w+rate*random.uniform(0.4,1.4)+random.uniform(-0.8,0.8)
            wt=round(w,1)
            if random.random()<0.06: wt=round(w/2.2046,1)  # entered in kg by mistake
            if random.random()<0.05: wt=""
            row=dict(client_id=messy_id(c["client_id"]),checkin_date=messy_date(d),weight=wt,avg_daily_steps=random.randint(3500,14000),avg_sleep_hours=round(random.uniform(5,8.5),1),energy_1to10=random.randint(3,10))
            ck.append(row)
            if random.random()<0.05: ck.append(dict(row))  # duplicate submission
        for day in range(7):
            dd=d+dt.timedelta(days=day)
            if random.random()<c["adherence"]:
                cal=random.randint(1700,3200); p=random.randint(110,240)
                if random.random()<0.03: cal=-cal  # bad entry
                nu.append(dict(client_id=messy_id(c["client_id"]),log_date=messy_date(dd),calories=cal,protein_g=p,carbs_g=random.randint(120,380),fat_g=random.randint(45,120)))
            if day in (0,2,4) and random.random()<c["adherence"]:
                for e in random.sample(exercises,3):
                    load=round(base[e]*(1+0.01*wk)/5)*5
                    wo.append(dict(client_id=messy_id(c["client_id"]),workout_date=messy_date(dd),exercise=e if random.random()>0.05 else e.upper(),sets=random.randint(3,5),reps=random.choice([5,6,8,10,12]),load_lbs=load))
def save(name,rows):
    with open(f"data/{name}.csv","w",newline="") as f:
        wr=csv.DictWriter(f,fieldnames=list(rows[0].keys())); wr.writeheader(); wr.writerows(rows)
for c in clients: c.pop("adherence")
save("clients",clients);save("checkins",ck);save("nutrition",nu);save("workouts",wo)
print(len(clients),len(ck),len(nu),len(wo))
