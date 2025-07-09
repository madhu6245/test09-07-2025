import multiprocessing
import time
import random
import pandas as pd
import matplotlib.pyplot as plt

NUM_WORKERS = 6
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
PERIODS_PER_DAY = 6
TOTAL_PERIODS_PER_WEEK = len(DAYS) * PERIODS_PER_DAY

teachers_data = {
    "T001": {"name": "Ms. Alice", "subjects": ["Math", "Science"]},
    "T002": {"name": "Mr. Bob", "subjects": ["English", "Social Studies"]},
    "T003": {"name": "Ms. Carol", "subjects": ["Math", "Physics"]},
    "T004": {"name": "Mr. David", "subjects": ["Chemistry", "Biology","Science"]},
    "T005": {"name": "Ms. Eve", "subjects": ["Computer Science", "Math"]},
    "T006": {"name": "Mr. Frank", "subjects": ["Art", "Music"]},
    "T007": {"name": "Ms. Grace", "subjects": ["Hindi", "English"]},
    "T008": {"name": "Mr. Harry", "subjects": ["Physical Education","Math"]},
    "T009": {"name": "Ms. Ivy", "subjects": ["Math", "Science", "Computer Science"]},
    "T010": {"name": "Mr. Jack", "subjects": ["Social Studies", "Hindi"]},
}

all_class_sections = []
for i in range(1, 11):
    all_class_sections.extend(
        f"Class{i}{section}"
        for section in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    )
subject_requirements_template = {
    "Math": 1, "Science": 1, "English": 1,
    "Social Studies": 1, "Computer Science": 1, "Art": 1 
}

teacher_capabilities = {}
for teacher_id, data in teachers_data.items():
    for subject in data["subjects"]:
        if subject not in teacher_capabilities:
            teacher_capabilities[subject] = []
        teacher_capabilities[subject].append(teacher_id)

def worker_timetable_allocator(
    worker_id,
    assigned_class_sections,
    teacher_period_locks,
    teacher_current_load,
    results_queue,
    subject_requirements_template,
    teacher_capabilities
): 
    print(f"Worker {worker_id} started. Assigned sections: {assigned_class_sections}")
    worker_timetable = [] 

    random.shuffle(assigned_class_sections)

    for class_section in assigned_class_sections:
        class_timetable_slots = {}
        for day in DAYS:
            for period in range(1, PERIODS_PER_DAY + 1):
                class_timetable_slots[(day, period)] = {"teacher": None, "subject": None}

        remaining_subjects_for_class = {
            subject: periods_needed_per_day * len(DAYS)
            for subject, periods_needed_per_day in subject_requirements_template.items()
        }

        for day in DAYS:
            for period_idx, period in enumerate(range(1, PERIODS_PER_DAY + 1)):
                if class_timetable_slots[(day, period)]["teacher"] is not None:
                    continue
                assigned_this_slot = False

                potential_assignments = []
                for subject, periods_remaining in remaining_subjects_for_class.items():
                    if periods_remaining > 0:
                        suitable_teachers = teacher_capabilities.get(subject, [])
                        suitable_teachers.sort(key=lambda t_id: teacher_current_load.get(t_id, 0))
                        potential_assignments.extend(
                            (subject, teacher_id)
                            for teacher_id in suitable_teachers
                        )
                random.shuffle(potential_assignments)
                for subject, teacher_id in potential_assignments:
                    lock_key = (teacher_id, day, period)
                    if teacher_period_locks[lock_key].acquire(timeout=0.5):
                        try:
                            if remaining_subjects_for_class[subject] > 0:
                                class_timetable_slots[(day, period)] = {
                                    "teacher": teacher_id,
                                    "subject": subject
                                }
                                remaining_subjects_for_class[subject] -= 1
                                teacher_current_load[teacher_id] += 1
                                worker_timetable.append({
                                    "class_section": class_section,
                                    "day": day,
                                    "period": period,
                                    "teacher": teacher_id,
                                    "subject": subject
                                })
                                assigned_this_slot = True
                                break
                        finally:
                            teacher_period_locks[lock_key].release()

                    if assigned_this_slot:
                        break
                if not assigned_this_slot:
                    print(f"Worker {worker_id} Warning: Could not assign a teacher for {class_section} on {day}, Period {period}. No suitable teacher or all busy for *any* remaining subject.")
                   
    print(f"Worker {worker_id} finished processing.")
    results_queue.put(worker_timetable)
    results_queue.put(worker_timetable)

def master_node():
    manager = multiprocessing.Manager()

    teacher_period_locks = manager.dict()
    teacher_current_load = manager.dict()
    results_queue = manager.Queue()

    for teacher_id in teachers_data.keys():
        teacher_current_load[teacher_id] = 0 
        for day in DAYS:
            for period in range(1, PERIODS_PER_DAY + 1):
                teacher_period_locks[(teacher_id, day, period)] = manager.Lock()

    class_sections_per_worker = [[] for _ in range(NUM_WORKERS)]
    for i, cs in enumerate(all_class_sections):
        class_sections_per_worker[i % NUM_WORKERS].append(cs)

    processes = []
    for i in range(NUM_WORKERS):
        p = multiprocessing.Process(
            target=worker_timetable_allocator,
            args=(
                i + 1,
                class_sections_per_worker[i],
                teacher_period_locks,
                teacher_current_load,
                results_queue,
                subject_requirements_template,
                teacher_capabilities
            )
        )
        processes.append(p)
        p.start()

    all_timetables = []
    for _ in processes:
        all_timetables.extend(results_queue.get())

    for p in processes:
        p.join()

    print("\nAll workers finished. Aggregating results and generating reports.")

    df_timetable = pd.DataFrame(all_timetables)

    print("\n--- Teacher-wise Timetable ---")
    for teacher_id, name in [(tid, data['name']) for tid, data in teachers_data.items()]:
        teacher_df = df_timetable[df_timetable['teacher'] == teacher_id].sort_values(by=['day', 'period'])
        print(f"\nTeacher: {name} ({teacher_id})")
        if not teacher_df.empty:
            display_df = teacher_df[['day', 'period', 'class_section', 'subject']]
            display_df['day'] = pd.Categorical(display_df['day'], categories=DAYS, ordered=True)
            print(display_df.sort_values(by=['day', 'period']).to_string(index=False))
        else:
            print("   No assignments.")

    print("\n--- Class-wise Timetable ---")
    for class_section in all_class_sections:
        class_df = df_timetable[df_timetable['class_section'] == class_section].sort_values(by=['day', 'period'])
        print(f"\nClass/Section: {class_section}")
        if not class_df.empty:
            display_df = class_df[['day', 'period', 'teacher', 'subject']]
            display_df['day'] = pd.Categorical(display_df['day'], categories=DAYS, ordered=True)
            print(display_df.sort_values(by=['day', 'period']).to_string(index=False))
        else:
            print("   No assignments.")

    print("\n--- Teacher Utilization Report ---")
    total_possible_periods_per_teacher = TOTAL_PERIODS_PER_WEEK
    teacher_utilization = {
        teacher_id: {
            "name": teachers_data[teacher_id]["name"],
            "assigned_periods": load,
            "utilization_percent": (load / total_possible_periods_per_teacher)
            * 100,
        }
        for teacher_id, load in teacher_current_load.items()
    }
    df_utilization = pd.DataFrame.from_dict(teacher_utilization, orient='index')
    print(df_utilization[['name', 'assigned_periods', 'utilization_percent']].sort_values(by='utilization_percent', ascending=False).to_string())

    plt.figure(figsize=(12, 6)) 
    plt.bar(df_utilization['name'], df_utilization['utilization_percent'], color="skyblue")
    plt.xlabel('Teacher')
    plt.ylabel('Utilization (%)')
    plt.title('Teacher Utilization Percentage')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    start_time = time.time()
    master_node()
    end_time = time.time()
    print(f"\nTotal execution time: {end_time - start_time:.2f} seconds")