
# 1. SparkSession은 Databricks에서 자동으로 생성됨 (spark 변수로 사용 가능)
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 2. 샘플 데이터 생성
sample_data = [
    ("Alice", 25, "Engineering", 75000),
    ("Bob", 30, "Sales", 65000),
    ("Charlie", 35, "Engineering", 80000),
    ("Diana", 28, "Marketing", 70000),
    ("Eve", 32, "Sales", 68000)
]

columns = ["name", "age", "department", "salary"]

# DataFrame 생성
df = spark.createDataFrame(sample_data, columns)

# 3. DataFrame을 임시 뷰로 등록 (SQL 쿼리에서 사용하기 위해)
df.createOrReplaceTempView("employees")

# 4. 기본 SQL 쿼리 실행 예제들

print("=== 1. 전체 데이터 조회 ===")
spark.sql("SELECT * FROM employees").show()

print("\n=== 2. 특정 컬럼 선택 ===")
spark.sql("SELECT name, department, salary FROM employees").show()

print("\n=== 3. 조건부 조회 (나이 30 이상) ===")
spark.sql("SELECT * FROM employees WHERE age >= 30").show()

print("\n=== 4. 그룹별 집계 (부서별 평균 급여) ===")
spark.sql("""
    SELECT department, 
           COUNT(*) as employee_count,
           AVG(salary) as avg_salary,
           MAX(salary) as max_salary
    FROM employees 
    GROUP BY department
    ORDER BY avg_salary DESC
""").show()

print("\n=== 5. 복잡한 쿼리 (급여 상위 3명) ===")
result = spark.sql("""
    SELECT name, department, salary,
           RANK() OVER (ORDER BY salary DESC) as salary_rank
    FROM employees
""")
result.show()

# 6. SQL 결과를 DataFrame으로 저장하여 추가 처리
high_earners = spark.sql("""
    SELECT * FROM employees 
    WHERE salary > 70000
    ORDER BY salary DESC
""")

print("\n=== 6. 고소득자 리스트 ===")
high_earners.show()

# 7. DataFrame API와 SQL 혼합 사용
print("\n=== 7. DataFrame API로 추가 처리 ===")
high_earners.select("name", "salary").withColumn("bonus", col("salary") * 0.1).show()

# 8. 다중 테이블 JOIN 예제를 위한 추가 데이터
bonus_data = [
    ("Alice", 5000),
    ("Bob", 3000),
    ("Charlie", 6000),
    ("Diana", 4000)
]

bonus_df = spark.createDataFrame(bonus_data, ["name", "bonus"])
bonus_df.createOrReplaceTempView("bonuses")

print("\n=== 8. JOIN 쿼리 ===")
spark.sql("""
    SELECT e.name, e.department, e.salary, 
           COALESCE(b.bonus, 0) as bonus,
           (e.salary + COALESCE(b.bonus, 0)) as total_compensation
    FROM employees e
    LEFT JOIN bonuses b ON e.name = b.name
    ORDER BY total_compensation DESC
""").show()

# 9. 윈도우 함수 사용 예제
print("\n=== 9. 윈도우 함수 (부서별 순위) ===")
spark.sql("""
    SELECT name, department, salary,
           ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
           DENSE_RANK() OVER (ORDER BY salary DESC) as overall_rank
    FROM employees
    ORDER BY department, dept_rank
""").show()

# 10. 결과를 파일로 저장 (선택사항)
# high_earners.write.mode("overwrite").parquet("/tmp/high_earners")
# print("결과가 /tmp/high_earners 에 저장되었습니다.")

print("PySpark SQL 예제 실행 완료!")
