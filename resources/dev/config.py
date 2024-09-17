##                                         DATA FILES LOCATION

BrazilEduPanel_Municipal_csv = "C:\\Users\\nitin\\assignment_file\\Brazil Education Panel Database\\BrazilEduPanel_Municipal.csv"
BrazilEduPanel_School_csv = "C:\\Users\\nitin\\assignment_file\\Brazil Education Panel Database\\BrazilEduPanel_School.csv"
BrazilEduPanel_Municipal_dta = "C:\\Users\\nitin\\assignment_file\\Brazil Education Panel Database\\BrazilEduPanel_Municipal.dta"
BrazilEduPanel_School_dta = "C:\\Users\\nitin\\assignment_file\\Brazil Education Panel Database\\BrazilEduPanel_School.dta"


#                                     SQL CREDENTIAL


database_name = "BrazilEduDB"
user = "root"
password = "nitin"

#                                DATABASE CONNECTION PARAMETERS

db_url = f"jdbc:mysql://localhost:3306/{database_name}"
db_properties = {
    "user": user,
    "password": password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

