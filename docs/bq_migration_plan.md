# BigQuery Migration Plan — Domain Intel

## Контекст

Система Domain Intel використовує BigQuery проект **`esoteric-parsec-147012`**, датасет **`es_analysis`**.

Частина таблиць вже існує і доступна через corp service account (read-only).  
Решта таблиць наразі створюється та ведеться через personal service account.

**Мета міграції:** надати corp service account права `BigQuery Data Editor` на датасет `es_analysis`,  
щоб всі операції (читання, запис, DML) виконувались через corp credentials.

---

## Необхідна дія від адміна BQ

**Надати роль** `BigQuery Data Editor` на датасет `esoteric-parsec-147012.es_analysis`  
для corp service account (email вказати окремо при запиті).

Роль включає: `bigquery.tables.create`, `bigquery.tables.get`, `bigquery.tables.updateData`,  
`bigquery.tables.delete`, `bigquery.jobs.create`.

---

## Таблиці що вже існують (corp read-only)

Ці таблиці вже створені. Зміна прав дозволить їх модифікувати при потребі.

| Таблиця | Опис |
|---|---|
| `similarweb_raw_data` | Кеш відповідей SimilarWeb API (JSON) |
| `builtwith_raw_data` | Кеш відповідей BuiltWith API (JSON) |
| `claude_responses` | Результати AI-класифікації доменів (JSON) |
| `latest_categories_claude` | VIEW на `claude_responses` — остання класифікація на домен |

---

## Таблиці що потребують створення

Ці таблиці зараз існують лише в personal BQ. Після міграції будуть створені автоматично при першому старті застосунку — **якщо corp SA матиме права `Data Editor`**.

---

### 1. `analysis_jobs`

**Призначення:** журнал пакетних завдань обробки доменів. Кожен запуск аналізу = один рядок.

| Поле | Тип | Опис |
|---|---|---|
| `job_id` | STRING (REQUIRED) | UUID завдання |
| `created_at` | TIMESTAMP | Час створення |
| `updated_at` | TIMESTAMP | Час останнього оновлення |
| `status` | STRING | `pending` / `running` / `completed` / `completed_with_errors` / `failed` / `cancelled` |
| `total_domains` | INTEGER | Загальна кількість доменів у завданні |
| `processed_domains` | INTEGER | Успішно оброблено |
| `failed_domains` | INTEGER | Завершено з помилкою |
| `services` | STRING | JSON-масив: `["similarweb","builtwith","ai"]` |
| `filename` | STRING | Назва завантаженого файлу |
| `error_message` | STRING | Текст помилки (якщо status=failed) |

---

### 2. `analysis_results`

**Призначення:** результати аналізу кожного домену. Один рядок = один домен в одному job-і.

| Поле | Тип | Опис |
|---|---|---|
| `job_id` | STRING | Посилання на `analysis_jobs.job_id` |
| `domain` | STRING | Домен (напр. `example.com`) |
| `processed_at` | TIMESTAMP | Час обробки |
| `status` | STRING | `ok` / `error` |
| `sw_visits` | FLOAT | Місячний трафік (SimilarWeb) |
| `cms_list` | STRING | Список CMS через кому (напр. `WordPress, WooCommerce`) |
| `osearch_group` | STRING | Група пошукової платформи |
| `osearch` | STRING | Назва пошукової платформи |
| `ems_list` | STRING | Список Email Marketing Systems |
| `ai_category` | STRING | AI-категорія (напр. `product_ecom`, `saas`) |
| `ai_is_ecommerce` | STRING | `Так` / `Ні` |
| `ai_industry` | STRING | AI-підкатегорія (напр. `fashion_accessories`) |
| `bw_vertical` | STRING | Вертикаль за даними BuiltWith |
| `bw_industry` | STRING | Галузь за даними BuiltWith |
| `sw_category` | STRING | Категорія за даними SimilarWeb |
| `sw_subcategory` | STRING | Підкатегорія за даними SimilarWeb |
| `sw_description` | STRING | Опис сайту (SimilarWeb) |
| `sw_title` | STRING | Назва сайту (SimilarWeb) |
| `sw_primary_region` | STRING | Основна країна трафіку (код, напр. `UA`) |
| `sw_primary_region_pct` | FLOAT | Частка трафіку з основної країни (0–100) |
| `company_name` | STRING | Назва компанії |
| `osearch_parse` | STRING | Сирі дані пошуку (JSON-рядок) |
| `sw_top_countries` | STRING | Топ-країни трафіку (JSON-рядок) |
| `bw_technologies` | STRING | Всі технології BuiltWith (JSON-рядок) |
| `bw_cms_raw` | STRING | Сирі CMS дані BuiltWith |
| `bw_ecommerce` | STRING | E-commerce платформа (BuiltWith) |
| `bw_email_marketing` | STRING | Email-платформа (BuiltWith) |
| `error_detail` | STRING | Деталі помилки обробки |

---

### 3. `domain_profiles`

**Призначення:** агрегований профіль домену — «останній відомий стан» по кожному домену незалежно від job-у. Використовується в Explorer для пошуку та фільтрації.

| Поле | Тип | Опис |
|---|---|---|
| `domain` | STRING | Домен (унікальний ключ) |
| `updated_at` | TIMESTAMP | Час останнього оновлення профілю |
| `sw_visits` | FLOAT | Місячний трафік |
| `sw_category` | STRING | Категорія (SimilarWeb) |
| `sw_subcategory` | STRING | Підкатегорія (SimilarWeb) |
| `sw_description` | STRING | Опис сайту |
| `sw_title` | STRING | Назва сайту |
| `sw_primary_region` | STRING | Основна країна трафіку |
| `sw_primary_region_pct` | FLOAT | Частка трафіку з основної країни |
| `company_name` | STRING | Назва компанії |
| `cms_list` | STRING | CMS через кому |
| `osearch` | STRING | Пошукова платформа |
| `osearch_group` | STRING | Група пошукової платформи |
| `ems_list` | STRING | Email Marketing Systems |
| `bw_vertical` | STRING | Вертикаль (BuiltWith) |
| `ai_category` | STRING | AI-категорія |
| `ai_is_ecommerce` | STRING | `Так` / `Ні` |
| `ai_industry` | STRING | AI-підкатегорія |

> **Примітка:** таблиця оновлюється методом MERGE (upsert) — вставляє новий рядок або оновлює існуючий по полю `domain`.

---

### 4. `app_users`

**Призначення:** облікові записи користувачів системи. Basic Auth + permissions.

| Поле | Тип | Опис |
|---|---|---|
| `username` | STRING | Логін (унікальний) |
| `password` | STRING | Пароль (plain text, не хешований) |
| `permissions` | STRING | Права через кому: `explorer,jobs,download,sheets,admin` |
| `created_at` | TIMESTAMP | Дата створення |
| `first_name` | STRING | Ім'я |
| `last_name` | STRING | Прізвище |
| `email` | STRING | Email (для майбутньої інтеграції з Google) |
| `google_folder` | STRING | Google Drive Folder ID для персональних експортів |
| `display_name` | STRING | Відображуване ім'я (необов'язково) |

> **Примітка:** поля `first_name`, `last_name`, `email`, `google_folder`, `display_name` додаються через `ALTER TABLE ADD COLUMN IF NOT EXISTS` при першому старті.

---

### 5. `activity_logs`

**Призначення:** журнал дій користувачів (аудит). Тільки запис (append-only).

| Поле | Тип | Опис |
|---|---|---|
| `logged_at` | TIMESTAMP | Час дії |
| `username` | STRING | Логін користувача |
| `action` | STRING | Код дії: `job_created`, `export_csv`, `export_xlsx`, `export_sheets`, `explore_export_xlsx`, `explore_export_sheets` |
| `details` | STRING | JSON-рядок з деталями (кількість доменів, job_id тощо) |

---

### 6. `sw_usage_counter`

**Призначення:** лічильник реальних API-запитів до SimilarWeb по користувачах та датах.

| Поле | Тип | Опис |
|---|---|---|
| `date` | DATE | Дата (YYYY-MM-DD) |
| `username` | STRING | Логін користувача |
| `api` | STRING | Назва API: `similarweb` / `builtwith` |
| `calls` | INTEGER | Кількість запитів за день |

> **Примітка:** оновлюється через MERGE (upsert) по `(date, username, api)`.

---

### 7. `app_settings`

**Призначення:** сховище ключ-значення для системних налаштувань та тимчасових даних.

| Поле | Тип | Опис |
|---|---|---|
| `key` | STRING (REQUIRED) | Ключ налаштування |
| `value` | STRING | Значення |
| `updated_at` | TIMESTAMP | Час останнього оновлення |

> **Приклади ключів:** `cache_ttl_days`, `sheet_url_{job_id}`, `builtwith_credits`, `similarweb_credits`

---

### 8. `technology_catalog`

**Призначення:** каталог відомих технологій для нормалізації даних BuiltWith. Синхронізується з Google Sheets.

| Поле | Тип | Опис |
|---|---|---|
| `sheet` | STRING | Розділ каталогу: `cms` / `ems` / `osearch` |
| `technology` | STRING | Назва технології (напр. `WordPress`, `Klaviyo`) |
| `group_name` | STRING | Група технологій (використовується для `osearch`) |

---

### 9. `redirect_cache`

**Призначення:** кеш розв'язаних редіректів доменів. Дозволяє уникати повторних HTTP-запитів при обробці.

| Поле | Тип | Опис |
|---|---|---|
| `original` | STRING | Вхідний домен |
| `resolved` | STRING | Кінцевий домен після редіректу |
| `type` | STRING | Тип редіректу: `redirect` / `alias` / `same` |
| `detected_at` | TIMESTAMP | Час виявлення |
| `job_id` | STRING | Job у якому виявлено редірект |

---

## Підсумок

| # | Таблиця | Статус | Записів (орієнтовно) |
|---|---|---|---|
| 1 | `similarweb_raw_data` | ✅ існує | млн+ |
| 2 | `builtwith_raw_data` | ✅ існує | млн+ |
| 3 | `claude_responses` | ✅ існує | тис. |
| 4 | `latest_categories_claude` | ✅ VIEW | — |
| 5 | `analysis_jobs` | 🔄 мігрувати | ~сотні |
| 6 | `analysis_results` | 🔄 мігрувати | ~млн |
| 7 | `domain_profiles` | 🔄 мігрувати | ~145k |
| 8 | `app_users` | 🔄 мігрувати | ~десятки |
| 9 | `activity_logs` | 🔄 мігрувати | ~тисячі |
| 10 | `sw_usage_counter` | 🔄 мігрувати | ~сотні |
| 11 | `app_settings` | 🔄 мігрувати | ~десятки |
| 12 | `technology_catalog` | 🔄 мігрувати | ~сотні |
| 13 | `redirect_cache` | 🔄 мігрувати | ~тисячі |

**Таблиці зі статусом 🔄 будуть створені автоматично** при першому старті застосунку після надання прав `BigQuery Data Editor` corp service account.  
Міграція існуючих даних — окремий крок (export/import через BQ Console або `bq` CLI).
