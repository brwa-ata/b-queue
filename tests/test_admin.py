from datetime import UTC, datetime

import pytest
from django.urls import reverse

from b_queue import schedule
from b_queue.admin import schedule_entries
from b_queue.models import Job
from b_queue.testing import run_jobs
from tests import tasks

SCHEDULES = reverse('admin:b_queue_schedulerun_changelist')
JOBS = reverse('admin:b_queue_job_changelist')


def utc(text):
    return datetime.strptime(text, '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC)


@pytest.fixture
def entries(db):
    schedule.task(tasks.add, 1, 2).every_minute().name('Add numbers')
    schedule.task(tasks.boom).every_minute().name('Broken')
    schedule.task(tasks.add, 5, b=6).daily_at('01:00').timezone('Asia/Baghdad').name(
        'Nightly'
    )


def by_name(rows):
    return {row['name']: row for row in rows}


def test_jobs_remember_their_schedule_entry(entries):
    schedule.run(utc('2026-01-01 10:00:00'))
    tasks.add.enqueue(8, 9)  # queued by hand: no entry

    names = Job.objects.order_by('enqueued_at').values_list('schedule', flat=True)
    assert list(names) == ['Add numbers', 'Broken', None]


def test_redis_jobs_remember_their_schedule_entry(alias, backend, on):
    schedule.task(on(tasks.add), 1, 2).every_minute().name('Add numbers')
    schedule.run(utc('2026-01-01 10:00:00'))

    record = backend.reserve(['*'], 'worker')
    assert record.schedule == 'Add numbers'


def test_entries_before_the_scheduler_ever_ran(entries):
    rows = by_name(schedule_entries(utc('2026-01-01 10:00:30')))

    assert list(rows) == ['Add numbers', 'Broken', 'Nightly']
    row = rows['Add numbers']
    assert row['task'] == 'tests.tasks.add'
    assert row['args'] == '1, 2'
    assert row['expression'] == '* * * * *'
    assert row['next_due'] == utc('2026-01-01 10:01:00')
    assert row['last_slot'] is None
    assert row['last_job'] is None
    assert (row['total'], row['failed']) == (0, 0)

    nightly = rows['Nightly']
    assert nightly['args'] == '5, b=6'
    assert nightly['timezone'] == 'Asia/Baghdad'
    assert nightly['next_due'] == utc('2026-01-01 22:00:00')  # 01:00 in Baghdad


def test_entries_show_what_became_of_their_jobs(entries):
    schedule.run(utc('2026-01-01 10:00:00'))
    run_jobs()
    schedule.run(utc('2026-01-01 10:01:00'))

    rows = by_name(schedule_entries(utc('2026-01-01 10:01:30')))

    added = rows['Add numbers']
    assert added['last_slot'] == utc('2026-01-01 10:01:00')
    assert (added['total'], added['failed']) == (2, 0)
    assert added['last_job'].status == 'READY'  # the 10:01 run is still queued

    broken = rows['Broken']
    assert (broken['total'], broken['failed']) == (2, 1)
    assert rows['Nightly']['total'] == 0


def test_entries_on_redis_are_listed_without_job_tracking(alias, on):
    schedule.task(on(tasks.add), 1, 2).hourly().name('Hourly')
    row = schedule_entries(utc('2026-01-01 10:00:30'))[0]

    assert row['next_due'] == utc('2026-01-01 11:00:00')
    assert row['tracked'] is (alias == 'default')


def test_schedules_page(entries, admin_client):
    schedule.run(utc('2026-01-01 10:00:00'))
    run_jobs()

    response = admin_client.get(SCHEDULES)
    assert response.status_code == 200
    page = response.text
    assert 'Add numbers' in page
    assert 'tests.tasks.boom' in page
    assert '1 failed' in page
    assert f'{JOBS}?schedule=Add+numbers' in page


def test_schedules_page_with_no_entries(admin_client):
    response = admin_client.get(SCHEDULES)
    assert 'No schedule entries are declared.' in response.text


def test_schedules_page_reports_duplicate_names(db, admin_client):
    schedule.task(tasks.add, 1, 2).hourly()
    schedule.task(tasks.add, 3, 4).daily()

    response = admin_client.get(SCHEDULES)
    assert response.status_code == 200
    assert 'unique names' in response.text


def test_schedules_page_needs_permission(db, client, django_user_model):
    staff = django_user_model.objects.create_user('staff', is_staff=True)
    client.force_login(staff)
    assert client.get(SCHEDULES).status_code == 403


def test_schedules_cannot_be_edited(entries, admin_client):
    assert admin_client.get(reverse('admin:b_queue_schedulerun_add')).status_code == 403


def test_jobs_page_filters_by_schedule_entry(entries, admin_client):
    schedule.run(utc('2026-01-01 10:00:00'))
    tasks.plain.enqueue('by hand')

    response = admin_client.get(JOBS, {'schedule': 'Broken'})
    assert response.status_code == 200
    jobs = response.context['cl'].result_list
    assert [job.task_path for job in jobs] == ['tests.tasks.boom']

    job = Job.objects.get(schedule='Broken')
    detail = admin_client.get(reverse('admin:b_queue_job_change', args=[job.pk]))
    assert detail.status_code == 200
