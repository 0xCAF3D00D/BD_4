from datetime import datetime, timezone
 
import pandas as pd

def get_days(published):
    parsed = datetime.strptime(published, "%Y-%m-%dT%X%z")
    diff = datetime.now(timezone.utc) - parsed
    return diff.days;

def regexp(expression):
    return df["name"].str.contains("(?i)" + expression)

def exclude(buffer, mask, clazz):
    left, right = buffer[mask], buffer[~mask]
    left["clazz"] = clazz
    return left, right

def fix_salary(df):
    df["min_salary"] = df["min_salary"].fillna(df.groupby("city")["min_salary"].transform("mean"))
    df["max_salary"] = df["max_salary"].fillna(df.groupby("city")["max_salary"].transform("mean"))

    df["min_salary"] = df["min_salary"].fillna(df["min_salary"].mean())
    df["max_salary"] = df["max_salary"].fillna(df["max_salary"].mean())
    return df 


source = "vac.csv"
output = "vac_classes.csv"
 
df = pd.read_csv(source)

df["city"] = df["city"].fillna("Не указано")
df["min_salary"] = df["min_salary"].fillna(df["max_salary"])
df["max_salary"] = df["max_salary"].fillna(df["min_salary"])
df["duties"] = df["duties"].fillna("Не указано")
df["requirements"] = df["requirements"].fillna("Не указано")
df["conditions"] = df["conditions"].fillna("Не указано")
df["key_skills"] = df["key_skills"].fillna("Не указано")
df["days"] = df['published'].apply(lambda x: get_days(x))
 
devGroup = regexp("тестировщик|qa|разработчик|программист|developer|software|programmer")
engineerGroup = regexp("инженер|engineer")
policeGroup = regexp("полицейский|оперуполномоченный|участковый")   
estateGroup = regexp("риелтор|риэлтор|недвижим|жильё|жилье|жилья|жилой|жилую")
operatorGroup = regexp("оператор")
workerGroup = regexp("грузчик|кладовщик|комплектовщик|упаковщик|сборщик|сборщица|фасовщик|склада|склад")
adminGroup = regexp("менеджер|администратор|админ|начальник|заместитель|директор|управляющий")
driverGroup = regexp("водитель")
financeGroup = regexp("промоутер|продавец|кассир|продажам|продаже|торговый|торговая|торговую|банковский|финансы|финансовые|финансовую|финансовый|инвестиции|investment|бухгалтер|бухгалтерию|маркет")
specialistGroup = regexp("столяр|слесарь|токарь|сварщик|шлифовщик|электрик")
 
df_buffer = df[:]
 
dev, df_buffer = exclude(df_buffer, devGroup, "Разработчики")
engineer, df_buffer = exclude(df_buffer, engineerGroup, "Инженеры")
police, df_buffer = exclude(df_buffer, policeGroup, "Полиция")
estate, df_buffer = exclude(df_buffer, estateGroup, "Недвижимость")
operator, df_buffer = exclude(df_buffer, operatorGroup, "Операторы")
worker, df_buffer = exclude(df_buffer, workerGroup, "Работники склада")
admin, df_buffer = exclude(df_buffer, adminGroup, "Администраторы")
driver, df_buffer = exclude(df_buffer, driverGroup, "Водители")
finiance, df_buffer = exclude(df_buffer, financeGroup, "Финансы")
specialist, df_buffer = exclude(df_buffer, specialistGroup, "Специалисты")

others = df_buffer
others["clazz"] = "Другие"
 
dev = fix_salary(dev)
engineer = fix_salary(engineer)
police = fix_salary(police)
estate = fix_salary(estate)
operator = fix_salary(operator)
worker = fix_salary(worker)
admin = fix_salary(admin)
driver = fix_salary(driver)
finiance = fix_salary(finiance)
specialist = fix_salary(specialist)
others = fix_salary(others)
 
df = pd.concat([dev, engineer, police, estate, operator, worker, admin, driver, finiance, specialist, others])
df.to_csv(output)