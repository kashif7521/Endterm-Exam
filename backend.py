import psycopg2
import os

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "kashif")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "1234")

def get_connection():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)

def setup_database():
    """Creates tables if they do not exist."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                employee_id SERIAL PRIMARY KEY,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                position VARCHAR(100) NOT NULL,
                manager_id INTEGER REFERENCES employees(employee_id)
            );

            CREATE TABLE IF NOT EXISTS goals (
                goal_id SERIAL PRIMARY KEY,
                employee_id INTEGER REFERENCES employees(employee_id) NOT NULL,
                manager_id INTEGER REFERENCES employees(employee_id) NOT NULL,
                description TEXT NOT NULL,
                due_date DATE NOT NULL,
                status VARCHAR(50) NOT NULL CHECK (status IN ('Draft', 'In Progress', 'Completed', 'Cancelled')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS tasks (
                task_id SERIAL PRIMARY KEY,
                goal_id INTEGER REFERENCES goals(goal_id) ON DELETE CASCADE NOT NULL,
                description TEXT NOT NULL,
                is_approved BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS feedback (
                feedback_id SERIAL PRIMARY KEY,
                goal_id INTEGER REFERENCES goals(goal_id) NOT NULL,
                manager_id INTEGER REFERENCES employees(employee_id) NOT NULL,
                comments TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error setting up database: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# --- CRUD Operations for Employees ---

def create_employee(first_name, last_name, position, manager_id=None):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO employees (first_name, last_name, position, manager_id) VALUES (%s, %s, %s, %s) RETURNING employee_id;", 
                       (first_name, last_name, position, manager_id))
        employee_id = cursor.fetchone()[0]
        conn.commit()
        return employee_id
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def get_employees():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM employees;")
    employees = cursor.fetchall()
    cursor.close()
    conn.close()
    return employees

def get_employee_by_id(employee_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM employees WHERE employee_id = %s;", (employee_id,))
    employee = cursor.fetchone()
    cursor.close()
    conn.close()
    return employee

def update_employee(employee_id, first_name, last_name, position, manager_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE employees SET first_name = %s, last_name = %s, position = %s, manager_id = %s WHERE employee_id = %s;", 
                       (first_name, last_name, position, manager_id, employee_id))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def delete_employee(employee_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM employees WHERE employee_id = %s;", (employee_id,))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

# --- CRUD Operations for Goals ---

def create_goal(employee_id, manager_id, description, due_date, status):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO goals (employee_id, manager_id, description, due_date, status) VALUES (%s, %s, %s, %s, %s) RETURNING goal_id;",
                       (employee_id, manager_id, description, due_date, status))
        goal_id = cursor.fetchone()[0]
        conn.commit()
        return goal_id
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def get_goals(employee_id=None, manager_id=None):
    conn = get_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM goals"
    params = []
    if employee_id:
        query += " WHERE employee_id = %s"
        params.append(employee_id)
    if manager_id:
        query += " WHERE manager_id = %s" if not params else " AND manager_id = %s"
        params.append(manager_id)
    cursor.execute(query, params)
    goals = cursor.fetchall()
    cursor.close()
    conn.close()
    return goals

def update_goal(goal_id, description, due_date, status):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE goals SET description = %s, due_date = %s, status = %s WHERE goal_id = %s;",
                       (description, due_date, status, goal_id))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def delete_goal(goal_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM goals WHERE goal_id = %s;", (goal_id,))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

# --- CRUD Operations for Tasks ---

def create_task(goal_id, description, is_approved=False):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO tasks (goal_id, description, is_approved) VALUES (%s, %s, %s) RETURNING task_id;",
                       (goal_id, description, is_approved))
        task_id = cursor.fetchone()[0]
        conn.commit()
        return task_id
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def get_tasks_by_goal(goal_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tasks WHERE goal_id = %s;", (goal_id,))
    tasks = cursor.fetchall()
    cursor.close()
    conn.close()
    return tasks

def update_task_approval(task_id, is_approved):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE tasks SET is_approved = %s WHERE task_id = %s;",
                       (is_approved, task_id))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def delete_task(task_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM tasks WHERE task_id = %s;", (task_id,))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

# --- CRUD Operations for Feedback ---

def create_feedback(goal_id, manager_id, comments):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO feedback (goal_id, manager_id, comments) VALUES (%s, %s, %s) RETURNING feedback_id;",
                       (goal_id, manager_id, comments))
        feedback_id = cursor.fetchone()[0]
        conn.commit()
        return feedback_id
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def get_feedback_by_goal(goal_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM feedback WHERE goal_id = %s;", (goal_id,))
    feedback = cursor.fetchall()
    cursor.close()
    conn.close()
    return feedback

# --- Business Insights Queries ---

def get_business_insights():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            -- Total number of employees
            SELECT COUNT(*) FROM employees;

            -- Total number of goals, tasks, and feedback entries
            SELECT COUNT(*) FROM goals;
            SELECT COUNT(*) FROM tasks;
            SELECT COUNT(*) FROM feedback;

            -- Average number of goals per employee
            SELECT AVG(goal_count) FROM (SELECT employee_id, COUNT(*) AS goal_count FROM goals GROUP BY employee_id) AS subquery;

            -- Min/Max due dates for goals
            SELECT MIN(due_date), MAX(due_date) FROM goals;
            
            -- Count of goals by status
            SELECT status, COUNT(*) FROM goals GROUP BY status;
            
            -- Average number of tasks per goal
            SELECT AVG(task_count) FROM (SELECT goal_id, COUNT(*) AS task_count FROM tasks GROUP BY goal_id) AS subquery;

            -- Most productive employee (based on completed goals)
            SELECT employee_id, COUNT(*) AS completed_goals FROM goals WHERE status = 'Completed' GROUP BY employee_id ORDER BY completed_goals DESC LIMIT 1;
            
            -- Employee with the most feedback
            SELECT g.employee_id, COUNT(f.feedback_id) AS feedback_count
            FROM goals g JOIN feedback f ON g.goal_id = f.goal_id
            GROUP BY g.employee_id
            ORDER BY feedback_count DESC
            LIMIT 1;

        """)
        results = [cursor.fetchall() for _ in range(9)] # Fetch results for each query
        conn.commit()
        return results
    except psycopg2.Error as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    setup_database()
    # You can add some sample data population here if needed for testing