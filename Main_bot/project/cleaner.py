from country_call import find_country
import re
from material_selection import find_materials


def data_clean(index,row):

#     for index, row in df.head(10).iterrows():
#     print(f"index: {index}")
    dimention_1=""

    inches=""
    pounds=""
    ounces=""
    lb=""
    product_id=""
    country=""
    material=[]
    quantity=""
    data=[]
    # idd = []
    # Finding Dimention 1
    product_id=row[1]
    idd=(product_id)
    if row[2]!=None:
        dimention_unit=row[2].split(" ")[-1]
        if dimention_unit=="inches":
            numbers = re.findall(r'\d+\.\d+|\d+', row[2])
            dimention_1=(len(numbers))
            inches = 1
            for num in numbers:
                inches *= float(num)
            diction={'unit':'inches','value':str(inches),'dimentionality':dimention_1}
            data.append(diction)   

     # Finding Dimention 2               
    if row[3]!=None:
        dimention_unit=row[3].split(" ")[-1]
        dimention_unit=dimention_unit.lower()
        if dimention_unit=="pounds":
            numbers = re.findall(r'\d+\.\d+|\d+', row[3])
        
            pounds=numbers[0]
            diction={'unit':'pounds','value':str(pounds),'dimentionality':1}
            data.append(diction)


    # Finding Dimention 3            

    if row[3]!=None:
        dimention_unit=row[3].split(" ")[-1]
        dimention_unit=dimention_unit.lower()
        if dimention_unit=="ounces":
            numbers = re.findall(r'\d+\.\d+|\d+', row[3])
     
            ounces=numbers[0]
            diction={'unit':'ounces','value':str(ounces),'dimentionality':1}
            data.append(diction)


    # Finding Dimention 4
    if row[2]!=None:
        dimention_unit=row[2].split("l")[-1]
        dimention_unit=dimention_unit.lower()
        if dimention_unit=="b":
            numbers = re.findall(r'\d+\.\d+|\d+', row[2])
   
            lb=numbers[0]
            diction={'unit':'lb','value':str(lb),'dimentionality':1}
            data.append(diction)


    if row[2]!=None:
        dimention_unit=row[2].split('"')[-1]
        dimention_unit=dimention_unit.lower()
        if dimention_unit=="h" or dimention_unit=="w" or dimention_unit=="l" or dimention_unit=="d":
            numbers = re.findall(r'\d+\.\d+|\d+', row[2])
            dimention_1=(len(numbers))
            inches = 1
            for num in numbers:
                inches *= float(num)
            # print(inches)
            # print(dimention_5)
            diction={'unit':'inches','value':str(inches),'dimentionality':dimention_1}
            data.append(diction)




    title=f"Index: {index}, Row data: {row[4]}"
    descrip=f"Index: {index}, Row data: {row[5]}"
    country=find_country(title)
    if country == None:
        country = find_country(descrip)
        if country!=None:
            diction={'unit':'Origin','value':country,'dimentionality':1}
            data.append(diction)
    # print(country)
    material=find_materials(title)
    if material == None:
        material = find_materials(descrip)
        diction={'unit':'materials','value':material,'dimentionality':1}
        data.append(diction)  

    # Use regular expressions to find the quantity information
    text=f"{title} {descrip}"
    match = re.search(r"(pack of|pk|pcs|pieces|set|qty|piece|set of|piece of)\s?(\d+)", text, flags=re.IGNORECASE)
    if match==None:
        match = re.search(r"(\d+)(\s?|\s*)(pack|pk|pcs|pieces|set|packs|sets|piece|Pair Pack|Pair Packs|Count|Counts)", text, flags=re.IGNORECASE)
    if match==None:
        match = re.search(r"numeric_(\d+)", text, flags=re.IGNORECASE)
    if match==None:
        match = re.search(r"(\d+)\s*-\s*(pk|pcs|pieces|set|piece|pack|packs|Pair Pack|Pair Packs|Count|Counts)", text, flags=re.IGNORECASE)

    if match:
        quantity = match.group()
        diction={'unit':'Quantity','value':str(quantity),'dimentionality':1}
        data.append(diction)
#         product_des.append(data)
#     main_data={
#         'id':idd,
#         'product_des':product_des    
#     }
    return idd,data