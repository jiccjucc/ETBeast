# PORTFOLIO EDITION — anonymized demonstration copy of ETBeast.
# Not the production configuration; rates, branding, and examples are illustrative.
# Never point this copy at a live payroll database or production Discord server.

"""ETBeastDemo Discord employee time clock and commission tracker."""
import asyncio
from contextlib import ExitStack
import csv
import io
import json
import hashlib
import logging
import tarfile
import tempfile
import shutil
import os
import sqlite3
from datetime import datetime, timedelta, timezone, date, time
from zoneinfo import ZoneInfo
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN', '')
GUILD_ID = int(os.getenv('GUILD_ID') or '0')
MANAGER_ROLE_ID = int(os.getenv('MANAGER_ROLE_ID') or '0')
EMPLOYEE_ROLE_ID = int(os.getenv('EMPLOYEE_ROLE_ID') or '0')
HOURLY_RATE_CENTS = int((Decimal(os.getenv('HOURLY_RATE', '15')) * 100).quantize(Decimal('1')))
COMMISSION_RATE = Decimal(os.getenv('COMMISSION_RATE', '0.01'))
DATA_DIR = Path(os.getenv('DATA_DIR', 'data'))
DATA_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR = DATA_DIR / 'proofs'
UPLOAD_DIR.mkdir(exist_ok=True)
DB = sqlite3.connect(DATA_DIR / 'etbeast_demo.sqlite3')
DB.row_factory = sqlite3.Row
DB.execute('PRAGMA journal_mode=WAL')
DB.execute('PRAGMA foreign_keys=ON')
DB.executescript('''
CREATE TABLE IF NOT EXISTS shifts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
  clock_in TEXT NOT NULL, clock_out TEXT,
  UNIQUE(id, guild_id, user_id)
);
CREATE INDEX IF NOT EXISTS shift_lookup ON shifts(guild_id,user_id,clock_out);
CREATE TABLE IF NOT EXISTS commissions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  shift_id INTEGER NOT NULL REFERENCES shifts(id),
  guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
  submitted_at TEXT NOT NULL, profit_cents INTEGER NOT NULL,
  rate TEXT NOT NULL, commission_cents INTEGER NOT NULL,
  attachment_path TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending', reviewed_by INTEGER,
  reviewed_at TEXT, review_message_id INTEGER
);
''')
DB.executescript("""
CREATE TABLE IF NOT EXISTS guild_settings (
 guild_id INTEGER PRIMARY KEY,
 hourly_rate_cents INTEGER NOT NULL,
 commission_rate TEXT NOT NULL,
 review_channel_id INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS employee_rates (
 guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
 hourly_rate_cents INTEGER NOT NULL, commission_rate TEXT NOT NULL,
 PRIMARY KEY(guild_id,user_id)
);
CREATE TABLE IF NOT EXISTS settings_audit (
 id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
 actor_id INTEGER NOT NULL, changed_at TEXT NOT NULL,
 setting TEXT NOT NULL, old_value TEXT NOT NULL, new_value TEXT NOT NULL
);
""")
# Non-destructive upgrade for databases created by earlier ETBeastDemo versions.
setting_columns = {r['name'] for r in DB.execute('PRAGMA table_info(guild_settings)')}
if 'log_channel_id' not in setting_columns:
    DB.execute('ALTER TABLE guild_settings ADD COLUMN log_channel_id INTEGER NOT NULL DEFAULT 0')
DB.commit()
cols = {r['name'] for r in DB.execute('PRAGMA table_info(shifts)')}
if 'hourly_rate_cents' not in cols:
    DB.execute('ALTER TABLE shifts ADD COLUMN hourly_rate_cents INTEGER')
# Keep legacy profit_cents intact; new submissions use a separate revenue_cents column.
commission_cols = {r['name'] for r in DB.execute('PRAGMA table_info(commissions)')}
if 'revenue_cents' not in commission_cols:
    DB.execute('ALTER TABLE commissions ADD COLUMN revenue_cents INTEGER')
if 'order_count' not in commission_cols:
    DB.execute('ALTER TABLE commissions ADD COLUMN order_count INTEGER')
if 'performance_channel_id' not in setting_columns:
    DB.execute('ALTER TABLE guild_settings ADD COLUMN performance_channel_id INTEGER NOT NULL DEFAULT 0')
DB.commit()
# V5: bonus ledger; void entries rather than deleting to preserve the audit trail.
DB.executescript("""
CREATE TABLE IF NOT EXISTS bonuses (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 guild_id INTEGER NOT NULL,
 employee_id INTEGER NOT NULL,
 amount_cents INTEGER NOT NULL CHECK(amount_cents > 0),
 reason TEXT NOT NULL,
 created_at TEXT NOT NULL,
 created_by INTEGER NOT NULL,
 voided_at TEXT,
 voided_by INTEGER,
 void_reason TEXT
);
CREATE INDEX IF NOT EXISTS bonus_payroll_idx ON bonuses(guild_id,created_at,employee_id);
""")
DB.commit()
# V6: auditable corrections and persistent alert deduplication.
DB.executescript("""
CREATE TABLE IF NOT EXISTS shift_audit (
 id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
 shift_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
 action TEXT NOT NULL, old_start TEXT, old_end TEXT,
 new_start TEXT, new_end TEXT, reason TEXT NOT NULL,
 actor_id INTEGER NOT NULL, created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS shift_audit_index ON shift_audit(guild_id,shift_id);
CREATE TABLE IF NOT EXISTS shift_alerts (
 guild_id INTEGER NOT NULL, shift_id INTEGER NOT NULL,
 alert_kind TEXT NOT NULL, sent_at TEXT NOT NULL,
 PRIMARY KEY(guild_id,shift_id,alert_kind)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_shift_per_employee
 ON shifts(guild_id,user_id) WHERE clock_out IS NULL;
""")
DB.commit()
# V7: incidents are separate from pay unless explicitly authorized using V39 deduction approval.
DB.executescript("""
CREATE TABLE IF NOT EXISTS incident_fines (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 guild_id INTEGER NOT NULL,
 employee_id INTEGER NOT NULL,
 proposed_amount_cents INTEGER NOT NULL CHECK(proposed_amount_cents > 0),
 reason TEXT NOT NULL,
 event_date TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending_review'
   CHECK(status IN ('pending_review','reviewed','dismissed')),
 created_at TEXT NOT NULL,
 created_by INTEGER NOT NULL,
 updated_at TEXT,
 updated_by INTEGER,
 resolution_note TEXT
);
CREATE INDEX IF NOT EXISTS incident_fines_lookup
 ON incident_fines(guild_id, employee_id, created_at);
CREATE TABLE IF NOT EXISTS incident_fine_audit (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 guild_id INTEGER NOT NULL,
 incident_id INTEGER NOT NULL,
 action TEXT NOT NULL,
 actor_id INTEGER NOT NULL,
 occurred_at TEXT NOT NULL,
 note TEXT NOT NULL
);
""")
DB.commit()
# V39: Authorized deductions are a distinct, immutable ledger. Merely creating or
# reviewing an incident does not reduce pay. Never backfill old reviewed incidents.
DB.executescript("""
CREATE TABLE IF NOT EXISTS fine_payroll_deductions (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 guild_id INTEGER NOT NULL, incident_id INTEGER NOT NULL,
 employee_id INTEGER NOT NULL, week_start TEXT NOT NULL,
 amount_cents INTEGER NOT NULL CHECK(amount_cents > 0),
 authorization_reference TEXT NOT NULL,
 approved_by INTEGER NOT NULL, approved_at TEXT NOT NULL,
 UNIQUE(guild_id, incident_id)
);
CREATE INDEX IF NOT EXISTS fine_deduction_week_idx
 ON fine_payroll_deductions(guild_id, week_start, employee_id);
""")
DB.commit()


# V19: immutable weekly payroll approval/payment ledger. Never deletes old payroll records.
DB.executescript("""
CREATE TABLE IF NOT EXISTS payroll_periods (
 guild_id INTEGER NOT NULL, week_start TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('approved','paid')),
 gross_cents INTEGER NOT NULL, fingerprint TEXT NOT NULL,
 snapshot_json TEXT NOT NULL,
 approved_at TEXT NOT NULL, approved_by INTEGER NOT NULL,
 paid_at TEXT, paid_by INTEGER, payment_reference TEXT,
 PRIMARY KEY(guild_id, week_start)
);
CREATE TABLE IF NOT EXISTS payroll_period_audit (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 guild_id INTEGER NOT NULL, week_start TEXT NOT NULL,
 actor_id INTEGER NOT NULL, action TEXT NOT NULL,
 occurred_at TEXT NOT NULL, details TEXT NOT NULL
);
""")
DB.commit()

# V25: append-only audit-friendly external payment ledger. Payments are not wages,
# and never change approved earnings or the weekly earnings fingerprint.
DB.executescript("""
CREATE TABLE IF NOT EXISTS employee_payments (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 guild_id INTEGER NOT NULL,
 employee_id INTEGER NOT NULL,
 week_start TEXT NOT NULL,
 amount_cents INTEGER NOT NULL CHECK(amount_cents > 0),
 method TEXT NOT NULL CHECK(method IN ('Zelle','Wire','Bank transfer','Cash','Other')),
 reference TEXT NOT NULL,
 recorded_at TEXT NOT NULL,
 recorded_by INTEGER NOT NULL,
 paid_on TEXT NOT NULL,
 voided_at TEXT,
 voided_by INTEGER,
 void_reason TEXT
);
CREATE INDEX IF NOT EXISTS payment_period_lookup ON employee_payments(guild_id,week_start,employee_id);
""")
DB.commit()

# V52: auditable exclusions for duplicate legacy commissions in finalized snapshots.
# These never overwrite the original approved snapshot or any external payment.
DB.executescript("""
CREATE TABLE IF NOT EXISTS payroll_duplicate_commission_exclusions (
 guild_id INTEGER NOT NULL, week_start TEXT NOT NULL,
 commission_id INTEGER NOT NULL, employee_id INTEGER NOT NULL,
 commission_cents INTEGER NOT NULL CHECK(commission_cents >= 0),
 topup_cents INTEGER NOT NULL CHECK(topup_cents >= 0),
 adjustment_reference TEXT NOT NULL,
 created_at TEXT NOT NULL,
 PRIMARY KEY(guild_id,week_start,commission_id)
);
CREATE INDEX IF NOT EXISTS payroll_exclusions_week_idx
 ON payroll_duplicate_commission_exclusions(guild_id,week_start,employee_id);
""")
DB.commit()

# V53: Weekly externally tracked hours (not fabricated clock-in/clock-out shifts).
# Entries are additive to clocked hours and always tied to a specific Sunday week.
DB.executescript("""
CREATE TABLE IF NOT EXISTS manual_weekly_hours (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 guild_id INTEGER NOT NULL,
 employee_id INTEGER NOT NULL,
 week_start TEXT NOT NULL,
 work_type TEXT NOT NULL CHECK(work_type IN ('streaming','packing')),
 hours_decimal TEXT NOT NULL,
 rate_cents INTEGER NOT NULL CHECK(rate_cents >= 0),
 wages_cents INTEGER NOT NULL CHECK(wages_cents >= 0),
 reason TEXT NOT NULL,
 created_at TEXT NOT NULL,
 created_by INTEGER NOT NULL,
 voided_at TEXT,
 voided_by INTEGER,
 void_reason TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS manual_weekly_hours_active_unique
 ON manual_weekly_hours(guild_id,employee_id,week_start,work_type)
 WHERE voided_at IS NULL;
CREATE INDEX IF NOT EXISTS manual_weekly_hours_week_idx
 ON manual_weekly_hours(guild_id,week_start,employee_id);
""")
DB.commit()

# V12: separate roles and per-submission format guarantees. Legacy shifts remain legacy.
STREAM_RATE_CENTS = 1500
PACK_RATE_CENTS = 1800
STREAM_COMMISSION_RATE = Decimal('0.01')
CHARACTER_MIN_RATE_CENTS = 2500
shift_columns = {r['name'] for r in DB.execute('PRAGMA table_info(shifts)')}
if 'work_type' not in shift_columns:
    DB.execute("ALTER TABLE shifts ADD COLUMN work_type TEXT NOT NULL DEFAULT 'legacy'")
commission_columns = {r['name'] for r in DB.execute('PRAGMA table_info(commissions)')}
for name, sql in {
    'stream_type': "ALTER TABLE commissions ADD COLUMN stream_type TEXT NOT NULL DEFAULT 'legacy'",
    'sales_per_hour_cents': 'ALTER TABLE commissions ADD COLUMN sales_per_hour_cents INTEGER',
    'character_hours': 'ALTER TABLE commissions ADD COLUMN character_hours TEXT',
    'guarantee_topup_cents': 'ALTER TABLE commissions ADD COLUMN guarantee_topup_cents INTEGER NOT NULL DEFAULT 0',
    'pack_size': 'ALTER TABLE commissions ADD COLUMN pack_size INTEGER',
    'break_count': 'ALTER TABLE commissions ADD COLUMN break_count INTEGER',
    'finished_at': 'ALTER TABLE commissions ADD COLUMN finished_at TEXT',
    'payroll_week_start': 'ALTER TABLE commissions ADD COLUMN payroll_week_start TEXT'
}.items():
    if name not in commission_columns:
        DB.execute(sql)
DB.commit()

# V26: preserve work-type/rate changes in the existing shift audit ledger.
shift_audit_columns = {r['name'] for r in DB.execute('PRAGMA table_info(shift_audit)')}
for name, sql in {
    'old_work_type': 'ALTER TABLE shift_audit ADD COLUMN old_work_type TEXT',
    'new_work_type': 'ALTER TABLE shift_audit ADD COLUMN new_work_type TEXT',
    'old_rate_cents': 'ALTER TABLE shift_audit ADD COLUMN old_rate_cents INTEGER',
    'new_rate_cents': 'ALTER TABLE shift_audit ADD COLUMN new_rate_cents INTEGER',
}.items():
    if name not in shift_audit_columns:
        DB.execute(sql)
DB.commit()

# V27: personal Streaming rates apply to future clock-ins only. Historical shift rates
# remain on each shift; changing this setting never rewrites paid time.
DB.executescript("""
CREATE TABLE IF NOT EXISTS employee_stream_rates (
 guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
 rate_cents INTEGER NOT NULL CHECK(rate_cents BETWEEN 1 AND 100000),
 set_at TEXT NOT NULL, set_by INTEGER NOT NULL,
 PRIMARY KEY(guild_id,user_id)
);
""")
DB.commit()

# V42: employee-specific Packing rates. These apply to new segments only;
# historic shifts retain their saved hourly_rate_cents.
DB.executescript("""
CREATE TABLE IF NOT EXISTS employee_pack_rates (
 guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
 rate_cents INTEGER NOT NULL CHECK(rate_cents BETWEEN 1 AND 100000),
 set_at TEXT NOT NULL, set_by INTEGER NOT NULL,
 PRIMARY KEY(guild_id,user_id)
);
""")
DB.commit()

# V22: one commission may carry multiple screenshots. Historical submissions keep
# their original attachment_path, so no existing screenshot needs migration.
DB.executescript("""
CREATE TABLE IF NOT EXISTS commission_attachments (
  commission_id INTEGER NOT NULL REFERENCES commissions(id) ON DELETE CASCADE,
  ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 1 AND 5),
  attachment_path TEXT NOT NULL,
  PRIMARY KEY (commission_id, ordinal)
);
""")
DB.commit()


# V28: persistent unpaid break ledger. Each break separates paid shift segments;
# payroll continues to read completed shifts unchanged. Older shifts are unaffected.
DB.executescript("""
CREATE TABLE IF NOT EXISTS employee_breaks (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 guild_id INTEGER NOT NULL,
 user_id INTEGER NOT NULL,
 prior_shift_id INTEGER NOT NULL REFERENCES shifts(id),
 next_shift_id INTEGER REFERENCES shifts(id),
 work_type TEXT NOT NULL CHECK(work_type IN ('streaming','packing')),
 hourly_rate_cents INTEGER NOT NULL,
 started_at TEXT NOT NULL,
 ended_at TEXT,
 ended_without_resuming INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_break_per_employee
 ON employee_breaks(guild_id,user_id) WHERE ended_at IS NULL;
CREATE INDEX IF NOT EXISTS break_employee_history ON employee_breaks(guild_id,user_id,started_at);
""")
DB.commit()


def open_break(guild_id, user_id):
    return DB.execute("""SELECT * FROM employee_breaks
        WHERE guild_id=? AND user_id=? AND ended_at IS NULL""",
        (guild_id,user_id)).fetchone()


def break_duration_text(start_iso, end_iso):
    seconds=max(0,(datetime.fromisoformat(end_iso)-datetime.fromisoformat(start_iso)).total_seconds())
    minutes=int(seconds//60)
    return f'{minutes//60}h {minutes%60}m'


def valid_commission_screenshots(attachments):
    """Validate count and format without trusting client-reported content type alone."""
    if not 1 <= len(attachments) <= 5:
        raise ValueError('Attach between 1 and 5 screenshots.')
    allowed = {'image/png': '.png', 'image/jpeg': '.jpg', 'image/webp': '.webp'}
    for idx, attachment in enumerate(attachments, 1):
        if attachment.size > 8 * 1024 * 1024 or attachment.size <= 0:
            raise ValueError(f'Screenshot {idx} must be between 1 byte and 8 MB.')
        if attachment.content_type not in allowed:
            raise ValueError(f'Screenshot {idx} must be PNG, JPG, or WebP.')
    return [allowed[a.content_type] for a in attachments]


def character_guarantee(revenue_cents: int, sales_per_hour_cents: int, duration_minutes=None, base_rate_cents=STREAM_RATE_CENTS):
    """Return (hours, commission, additional guarantee) using decimal arithmetic.

    The base hourly rate is paid in the clocked shift. The top-up adds only the
    shortfall between the example base plus commission and the example minimum.
    """
    if revenue_cents < 0 or sales_per_hour_cents < 0:
        raise ValueError('Revenue and sales/hour cannot be negative.')
    if duration_minutes is not None:
        if not 1 <= duration_minutes <= 1440:
            raise ValueError('Duration must be from 1 to 1440 minutes.')
        hours = Decimal(duration_minutes) / Decimal('60')
        if sales_per_hour_cents:
            derived_rate = Decimal(revenue_cents) / hours
            if abs(derived_rate - Decimal(sales_per_hour_cents)) > max(Decimal(100), Decimal(sales_per_hour_cents) * Decimal('0.02')):
                raise ValueError('Revenue, sales/hour and duration disagree by more than 2%. Check the screenshot.')
    else:
        if sales_per_hour_cents <= 0 or revenue_cents <= 0:
            raise ValueError('For a zero-revenue Character Break, ask a manager to document the duration and guarantee manually. Do not submit $0 sales/hour.')
        hours = Decimal(revenue_cents) / Decimal(sales_per_hour_cents)
    if hours <= 0 or hours > 24:
        raise ValueError('Calculated character-break duration must be between 0 and 24 hours.')
    commission = int((Decimal(revenue_cents) * STREAM_COMMISSION_RATE).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    guarantee = (hours * CHARACTER_MIN_RATE_CENTS).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
    base = (hours * base_rate_cents).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
    topup = max(0, int(guarantee) - int(base) - commission)
    return hours, commission, topup


# V20: fixed Character Break duration. Store pack size/count per submission so
# payroll and manager reviews can independently verify the rule used.
CHARACTER_PACK_MINUTES = {20: 40, 50: 90, 100: 120}


def character_pack_pay(revenue_cents: int, pack_size: int, break_count: int, base_rate_cents=STREAM_RATE_CENTS):
    """Return (hours, commission cents, extra top-up cents)."""
    if pack_size not in CHARACTER_PACK_MINUTES:
        raise ValueError('Choose 20, 50, or 100 packs.')
    if not isinstance(break_count, int) or not 1 <= break_count <= 20:
        raise ValueError('Break count must be between 1 and 20.')
    if revenue_cents < 0:
        raise ValueError('Revenue cannot be negative.')
    minutes = CHARACTER_PACK_MINUTES[pack_size] * break_count
    if minutes > 24 * 60:
        raise ValueError('Total submitted Character Break duration exceeds 24 hours; submit fewer breaks.')
    hours = Decimal(minutes) / Decimal(60)
    commission = int((Decimal(revenue_cents) * STREAM_COMMISSION_RATE).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    guarantee = int((hours * CHARACTER_MIN_RATE_CENTS).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    base = int((hours * base_rate_cents).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    topup = max(0, guarantee - base - commission)
    return hours, commission, topup


def character_hours_claimed(shift_id, *, excluding=None, approved_only=False):
    sql = "SELECT character_hours FROM commissions WHERE shift_id=? AND stream_type='character_break' AND status!='rejected' AND character_hours IS NOT NULL"
    args=[shift_id]
    if excluding is not None:
        sql += ' AND id!=?'
        args.append(excluding)
    if approved_only:
        sql += " AND status='approved'"
    return sum((Decimal(r['character_hours']) for r in DB.execute(sql,args)), Decimal('0'))


def shift_elapsed_hours(shift):
    ending = datetime.fromisoformat(shift['clock_out']) if shift['clock_out'] else datetime.now(timezone.utc)
    return Decimal(str((ending - datetime.fromisoformat(shift['clock_in'])).total_seconds())) / Decimal('3600')


lock = asyncio.Lock()
logging.basicConfig(level=logging.INFO)


def settings_for(guild_id):
    row = DB.execute('SELECT * FROM guild_settings WHERE guild_id=?', (guild_id,)).fetchone()
    if row:
        return dict(row)
    return dict(guild_id=guild_id, hourly_rate_cents=HOURLY_RATE_CENTS,
                commission_rate=str(COMMISSION_RATE),
                review_channel_id=int(os.getenv('REVIEW_CHANNEL_ID') or '0'),
                log_channel_id=int(os.getenv('LOG_CHANNEL_ID') or '0'),
                performance_channel_id=int(os.getenv('PERFORMANCE_CHANNEL_ID') or '0'))


def rates_for(guild_id, user_id):
    row = DB.execute('SELECT * FROM employee_rates WHERE guild_id=? AND user_id=?',
                     (guild_id, user_id)).fetchone()
    if row:
        return int(row['hourly_rate_cents']), Decimal(row['commission_rate'])
    cfg = settings_for(guild_id)
    return int(cfg['hourly_rate_cents']), Decimal(cfg['commission_rate'])


def parse_nonnegative(value, max_value):
    try:
        result = Decimal(value.strip().replace(',', ''))
        if not result.is_finite() or result < 0 or result > Decimal(max_value):
            raise ValueError
        return result
    except (ValueError, InvalidOperation):
        raise ValueError('Enter a nonnegative number within the allowed range.')


def audit_setting(guild_id, actor_id, key, old, new):
    DB.execute('INSERT INTO settings_audit(guild_id,actor_id,changed_at,setting,old_value,new_value) '
               'VALUES(?,?,?,?,?,?)', (guild_id, actor_id, now_iso(), key, str(old), str(new)))


def save_guild_setting(guild_id, actor_id, field, value):
    if field not in ('hourly_rate_cents', 'commission_rate', 'review_channel_id', 'log_channel_id', 'performance_channel_id'):
        raise ValueError('Unsupported setting')
    previous = settings_for(guild_id)
    cfg = dict(previous)
    cfg[field] = value
    DB.execute('INSERT INTO guild_settings (guild_id,hourly_rate_cents,commission_rate,review_channel_id,log_channel_id,performance_channel_id) '
               'VALUES (?,?,?,?,?,?) ON CONFLICT(guild_id) DO UPDATE SET '
               'hourly_rate_cents=excluded.hourly_rate_cents,commission_rate=excluded.commission_rate,'
               'review_channel_id=excluded.review_channel_id,log_channel_id=excluded.log_channel_id,'
               'performance_channel_id=excluded.performance_channel_id',
               (guild_id, cfg['hourly_rate_cents'], str(cfg['commission_rate']), cfg['review_channel_id'], cfg['log_channel_id'], cfg['performance_channel_id']))
    audit_setting(guild_id, actor_id, field, previous[field], value)
    DB.commit()


def configuration_embed(guild_id):
    cfg = settings_for(guild_id)
    overrides = DB.execute('SELECT COUNT(*) AS count FROM employee_rates WHERE guild_id=?',
                           (guild_id,)).fetchone()['count']
    embed = discord.Embed(title='⚙️ ETBeastDemo Manager Settings',
                          description='Only managers can use this panel. Changes save immediately.',
                          color=discord.Color.blurple())
    embed.add_field(name='Default hourly pay', value=money(cfg['hourly_rate_cents']))
    embed.add_field(name='Default commission', value=f"{Decimal(cfg['commission_rate']) * 100:g}%")
    channel_id = cfg['review_channel_id']
    embed.add_field(name='Review channel', value=f'<#{channel_id}>' if channel_id else 'Not configured')
    log_channel_id = cfg['log_channel_id']
    embed.add_field(name='Employee logs', value=f'<#{log_channel_id}>' if log_channel_id else 'Not configured')
    performance_channel_id = cfg['performance_channel_id']
    embed.add_field(name='Performance channel', value=f'<#{performance_channel_id}>' if performance_channel_id else 'Not configured')
    embed.add_field(name='Individual employee overrides', value=str(overrides), inline=False)
    embed.set_footer(text='New commissions are revenue-based. Legacy profit submissions retain their original records.')
    return embed


class DefaultPayModal(discord.ui.Modal, title='Default employee pay'):
    hourly = discord.ui.TextInput(label='Hourly rate (USD)', placeholder='15.00', max_length=12)
    percent = discord.ui.TextInput(label='Commission percentage', placeholder='5', max_length=10)

    async def on_submit(self, interaction):
        if not allowed_guild(interaction) or not manager(interaction):
            await interaction.response.send_message('Manager permission required.', ephemeral=True)
            return
        try:
            hourly = parse_nonnegative(str(self.hourly), '100000')
            percent = parse_nonnegative(str(self.percent), '100')
        except ValueError as exc:
            await interaction.response.send_message(str(exc), ephemeral=True)
            return
        async with lock:
            save_guild_setting(interaction.guild_id, interaction.user.id, 'hourly_rate_cents',
                               int((hourly * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP)))
            save_guild_setting(interaction.guild_id, interaction.user.id, 'commission_rate', str(percent / 100))
        await interaction.response.send_message('✅ Default pay updated. Run `/settings` to see current values.', ephemeral=True)


class ReviewChannelModal(discord.ui.Modal, title='Commission review channel'):
    channel_id = discord.ui.TextInput(label='Text channel ID', placeholder='CHANNEL_ID_HERE', max_length=22)

    async def on_submit(self, interaction):
        if not allowed_guild(interaction) or not manager(interaction):
            await interaction.response.send_message('Manager permission required.', ephemeral=True)
            return
        try:
            channel_id = int(str(self.channel_id).strip())
        except ValueError:
            channel_id = 0
        channel = interaction.guild.get_channel(channel_id) if channel_id else None
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message('Enter a valid text channel ID in this server.', ephemeral=True)
            return
        me = interaction.guild.me
        permissions = channel.permissions_for(me)
        if not (permissions.view_channel and permissions.send_messages and permissions.attach_files and permissions.embed_links):
            await interaction.response.send_message('Give the bot permission to view, send, attach files, and embed links in that channel.', ephemeral=True)
            return
        async with lock:
            save_guild_setting(interaction.guild_id, interaction.user.id, 'review_channel_id', channel_id)
        await interaction.response.send_message(f'✅ Commission review channel set to {channel.mention}.', ephemeral=True)


class LogChannelModal(discord.ui.Modal, title='Employee clock logs channel'):
    channel_id = discord.ui.TextInput(label='Private text channel ID', placeholder='CHANNEL_ID_HERE', max_length=22)

    async def on_submit(self, interaction):
        if not allowed_guild(interaction) or not manager(interaction):
            await interaction.response.send_message('Manager permission required.', ephemeral=True)
            return
        try:
            channel_id = int(str(self.channel_id).strip())
        except ValueError:
            channel_id = 0
        channel = interaction.guild.get_channel(channel_id) if channel_id else None
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message('Choose a text channel in this server. Enable Developer Mode to copy its channel ID.', ephemeral=True)
            return
        me = interaction.guild.me
        permissions = channel.permissions_for(me)
        if not (permissions.view_channel and permissions.send_messages and permissions.embed_links):
            await interaction.response.send_message('Grant the bot View Channel, Send Messages and Embed Links in that channel.', ephemeral=True)
            return
        async with lock:
            save_guild_setting(interaction.guild_id, interaction.user.id, 'log_channel_id', channel_id)
        await interaction.response.send_message(f'✅ Clock event logs will be posted in {channel.mention}. Make sure this channel is private to managers.', ephemeral=True)


class PerformanceChannelModal(discord.ui.Modal, title='Employee performance channel'):
    channel_id = discord.ui.TextInput(label='Private text channel ID', placeholder='CHANNEL_ID_HERE', max_length=22)

    async def on_submit(self, interaction):
        if not allowed_guild(interaction) or not manager(interaction):
            await interaction.response.send_message('Manager permission required.', ephemeral=True)
            return
        try:
            channel = interaction.guild.get_channel(int(str(self.channel_id).strip()))
        except ValueError:
            channel = None
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message('Choose a text channel in this server.', ephemeral=True)
            return
        perms = channel.permissions_for(interaction.guild.me)
        if not (perms.view_channel and perms.send_messages and perms.embed_links):
            await interaction.response.send_message('Bot needs View Channel, Send Messages and Embed Links.', ephemeral=True)
            return
        async with lock:
            save_guild_setting(interaction.guild_id, interaction.user.id, 'performance_channel_id', channel.id)
        await interaction.response.send_message(f'✅ Performance reports will post in {channel.mention}. Keep it manager-only.', ephemeral=True)


class EmployeeRateModal(discord.ui.Modal, title='Employee pay override'):
    user_id = discord.ui.TextInput(label='Employee Discord user ID', max_length=22)
    hourly = discord.ui.TextInput(label='Hourly rate (USD)', placeholder='15.00', max_length=12)
    percent = discord.ui.TextInput(label='Commission percentage', placeholder='5', max_length=10)

    async def on_submit(self, interaction):
        if not allowed_guild(interaction) or not manager(interaction):
            await interaction.response.send_message('Manager permission required.', ephemeral=True)
            return
        try:
            user_id = int(str(self.user_id).strip())
            hourly = parse_nonnegative(str(self.hourly), '100000')
            percent = parse_nonnegative(str(self.percent), '100')
        except (ValueError, TypeError):
            await interaction.response.send_message('Enter a valid user ID, hourly pay and percentage (0–100).', ephemeral=True)
            return
        member = interaction.guild.get_member(user_id)
        if member is None:
            try:
                member = await interaction.guild.fetch_member(user_id)
            except discord.HTTPException:
                pass
        if member is None:
            await interaction.response.send_message('Employee must be a member of this Discord server.', ephemeral=True)
            return
        cents = int((hourly * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
        rate = str(percent / 100)
        async with lock:
            old = DB.execute('SELECT * FROM employee_rates WHERE guild_id=? AND user_id=?',
                             (interaction.guild_id, user_id)).fetchone()
            DB.execute('INSERT INTO employee_rates (guild_id,user_id,hourly_rate_cents,commission_rate) '
                       'VALUES(?,?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET '
                       'hourly_rate_cents=excluded.hourly_rate_cents,commission_rate=excluded.commission_rate',
                       (interaction.guild_id,user_id,cents,rate))
            audit_setting(interaction.guild_id, interaction.user.id, f'employee:{user_id}',
                          dict(old) if old else 'default', f'{cents} cents/hour; {rate} commission')
            DB.commit()
        await interaction.response.send_message(f'✅ Custom pay saved for {member.mention}.', ephemeral=True)


class SettingsView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)

    async def interaction_check(self, interaction):
        if not allowed_guild(interaction) or not manager(interaction):
            await interaction.response.send_message('Manager permission required.', ephemeral=True)
            return False
        return True

    @discord.ui.button(label='Default Pay', style=discord.ButtonStyle.primary, emoji='💵')
    async def pay(self, interaction, button):
        cfg = settings_for(interaction.guild_id)
        modal = DefaultPayModal()
        modal.hourly.default = str(Decimal(cfg['hourly_rate_cents']) / 100)
        modal.percent.default = str(Decimal(cfg['commission_rate']) * 100)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label='Review Channel', style=discord.ButtonStyle.secondary, emoji='📷')
    async def review(self, interaction, button):
        await interaction.response.send_modal(ReviewChannelModal())

    @discord.ui.button(label='Employee Logs', style=discord.ButtonStyle.secondary, emoji='📋')
    async def employee_logs(self, interaction, button):
        await interaction.response.send_modal(LogChannelModal())

    @discord.ui.button(label='Performance', style=discord.ButtonStyle.secondary, emoji='📈')
    async def performance_channel(self, interaction, button):
        await interaction.response.send_modal(PerformanceChannelModal())

    @discord.ui.button(label='Employee Pay', style=discord.ButtonStyle.secondary, emoji='👤')
    async def employee(self, interaction, button):
        await interaction.response.send_modal(EmployeeRateModal())

    @discord.ui.button(label='Refresh', style=discord.ButtonStyle.secondary, emoji='🔄')
    async def refresh(self, interaction, button):
        await interaction.response.edit_message(embed=configuration_embed(interaction.guild_id), view=self)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def money(cents):
    return f'${cents / 100:,.2f}'


def manager(interaction):
    if not isinstance(interaction.user, discord.Member):
        return False
    return interaction.user.guild_permissions.manage_guild or (
        MANAGER_ROLE_ID and any(role.id == MANAGER_ROLE_ID for role in interaction.user.roles)
    )


def employee(interaction):
    if not isinstance(interaction.user, discord.Member):
        return False
    return not EMPLOYEE_ROLE_ID or manager(interaction) or any(
        role.id == EMPLOYEE_ROLE_ID for role in interaction.user.roles
    )


def allowed_guild(interaction):
    return bool(interaction.guild_id) and (not GUILD_ID or interaction.guild_id == GUILD_ID)


# V33: Durable employee-log outbox. Retries survive Discord outages and bot restarts.
# Events have a stable key to avoid queue duplicates; a successful send is marked sent.
# Note: Discord does not offer exactly-once sends: a crash after sending but before
# saving the message ID can produce a duplicate, so include event ID in each footer.
DB.executescript("""
CREATE TABLE IF NOT EXISTS employee_log_outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_key TEXT NOT NULL UNIQUE,
  guild_id INTEGER NOT NULL,
  channel_id INTEGER NOT NULL,
  employee_id INTEGER NOT NULL,
  employee_name TEXT NOT NULL,
  shift_id INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  clock_in TEXT NOT NULL,
  clock_out TEXT,
  hourly_rate_cents INTEGER,
  work_type TEXT NOT NULL,
  created_at TEXT NOT NULL,
  sent_at TEXT,
  message_id INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  next_attempt_at TEXT
);
CREATE INDEX IF NOT EXISTS employee_log_outbox_due
ON employee_log_outbox(sent_at,next_attempt_at,id);
""")
DB.commit()


def enqueue_clock_log(guild_id, user_id, display_name, action, shift_id, clock_in_iso,
                      clock_out_iso=None, hourly_rate_cents=None, event_type=None):
    channel_id = settings_for(guild_id)['log_channel_id']
    if not channel_id:
        logging.warning('No employee log channel configured for guild %s', guild_id)
        return None
    role = DB.execute('SELECT work_type FROM shifts WHERE id=? AND guild_id=?',
                      (shift_id, guild_id)).fetchone()
    kind = role['work_type'] if role else 'legacy'
    event = event_type or ('clock_in' if action == 'in' else 'clock_out')
    key = f'{guild_id}:{shift_id}:{event}'
    with DB:
        DB.execute("""INSERT OR IGNORE INTO employee_log_outbox
          (event_key,guild_id,channel_id,employee_id,employee_name,shift_id,
           event_type,clock_in,clock_out,hourly_rate_cents,work_type,created_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
          (key,guild_id,channel_id,user_id,display_name,shift_id,event,
           clock_in_iso,clock_out_iso,hourly_rate_cents,kind,now_iso()))
    row = DB.execute('SELECT id FROM employee_log_outbox WHERE event_key=?', (key,)).fetchone()
    return row['id'] if row else None


def employee_log_embed(event):
    kind = event['event_type']
    titles = {
        'clock_in': '🟢 Employee clocked in',
        'clock_out': '🔴 Employee clocked out',
        'break_start': '🍽️ Started meal break (unpaid)',
        'break_resume': '🟢 Resumed from meal break',
        'switch_out': '🔄 Work type switched (previous segment ended)',
        'switch_in': '🔄 Work type switched (new segment started)',
        'manager_force_out': '🔒 Manager force clock-out',
    }
    incoming = kind in ('clock_in','break_resume','switch_in')
    embed = discord.Embed(title=titles.get(kind,kind),
                          color=discord.Color.green() if incoming else discord.Color.orange())
    embed.add_field(name='Employee',value=discord.utils.escape_markdown(event['employee_name']))
    embed.add_field(name='Work type',value=event['work_type'].title())
    stamp = event['clock_in'] if incoming else event['clock_out']
    embed.add_field(name='Time',value=f'<t:{int(datetime.fromisoformat(stamp).timestamp())}:F>')
    if not incoming and event['clock_out'] and event['hourly_rate_cents'] is not None:
        duration = Decimal(str((datetime.fromisoformat(event['clock_out'])-
                                datetime.fromisoformat(event['clock_in'])).total_seconds()))/Decimal('3600')
        cents = int((duration*int(event['hourly_rate_cents'])).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
        embed.add_field(name='Paid segment',value=f'{duration:.2f} hours · {money(cents)} estimated base')
    embed.set_footer(text=f'Shift #{event["shift_id"]} · Event #{event["id"]} · Payroll subject to review')
    return embed


async def deliver_employee_log(event_id):
    event = DB.execute('SELECT * FROM employee_log_outbox WHERE id=?', (event_id,)).fetchone()
    if not event or event['sent_at']:
        return
    guild = bot.get_guild(event['guild_id'])
    channel = guild.get_channel(event['channel_id']) if guild else None
    try:
        if not isinstance(channel, discord.TextChannel):
            raise RuntimeError('Configured employee log channel is inaccessible')
        message = await channel.send(embed=employee_log_embed(event),
                                     allowed_mentions=discord.AllowedMentions.none())
    except (discord.HTTPException, RuntimeError) as exc:
        attempts = int(event['attempts'])+1
        delay = min(3600, 15*(2**min(attempts-1,8)))
        with DB:
            DB.execute("""UPDATE employee_log_outbox SET attempts=?,last_error=?,next_attempt_at=?
                WHERE id=? AND sent_at IS NULL""",
                (attempts,str(exc)[:400],(datetime.now(timezone.utc)+timedelta(seconds=delay)).isoformat(),event_id))
        logging.warning('Employee log #%s pending retry: %s',event_id,exc)
    else:
        with DB:
            DB.execute("""UPDATE employee_log_outbox SET sent_at=?,message_id=?,last_error=NULL,next_attempt_at=NULL
                WHERE id=?""",(now_iso(),message.id,event_id))


async def post_clock_log(interaction, action, shift_id, clock_in_iso, clock_out_iso=None,
                         hourly_rate_cents=None, event_type=None):
    """Persist the log event first; deliver now if possible, or retry later."""
    event_id = enqueue_clock_log(interaction.guild_id,interaction.user.id,
       interaction.user.display_name,action,shift_id,clock_in_iso,clock_out_iso,
       hourly_rate_cents,event_type)
    if event_id is not None:
        await deliver_employee_log(event_id)


@tasks.loop(seconds=30)
async def employee_log_retry_watch():
    now=now_iso()
    pending=DB.execute("""SELECT id FROM employee_log_outbox WHERE sent_at IS NULL
         AND (next_attempt_at IS NULL OR next_attempt_at<=?) ORDER BY id LIMIT 30""",(now,)).fetchall()
    for item in pending:
        await deliver_employee_log(item['id'])


@employee_log_retry_watch.before_loop
async def before_employee_log_retry_watch():
    await bot.wait_until_ready()


def streaming_rate_for_employee(guild_id, user_id):
    row = DB.execute('SELECT rate_cents FROM employee_stream_rates WHERE guild_id=? AND user_id=?',
                     (guild_id, user_id)).fetchone()
    return int(row['rate_cents']) if row else STREAM_RATE_CENTS


def packing_rate_for_employee(guild_id, user_id):
    row = DB.execute('SELECT rate_cents FROM employee_pack_rates WHERE guild_id=? AND user_id=?',
                     (guild_id, user_id)).fetchone()
    return int(row['rate_cents']) if row else PACK_RATE_CENTS


def shift_rate_for_employee(guild_id, user_id, work_type):
    if work_type == 'streaming':
        return streaming_rate_for_employee(guild_id, user_id)
    if work_type == 'packing':
        return packing_rate_for_employee(guild_id, user_id)
    raise ValueError('Choose Streaming or Packing.')


async def clock_in(interaction, work_type="streaming"):
    if not allowed_guild(interaction) or not employee(interaction):
        await interaction.response.send_message('You are not authorized to clock in here.', ephemeral=True)
        return
    if work_type not in ('streaming','packing'):
        await interaction.response.send_message('Choose Streaming or Packing.',ephemeral=True)
        return
    new_shift = None
    async with lock:
        previous = DB.execute(
            'SELECT id FROM shifts WHERE guild_id=? AND user_id=? AND clock_out IS NULL',
            (interaction.guild_id, interaction.user.id)
        ).fetchone()
        if previous:
            result = 'You are already clocked in.'
        elif open_break(interaction.guild_id, interaction.user.id):
            result = 'You are on a break. Use **End Break / Resume** to continue your saved shift, or Clock Out to finish your day.'
        else:
            started = now_iso()
            rate_cents = shift_rate_for_employee(interaction.guild_id, interaction.user.id, work_type)
            cursor = DB.execute('INSERT INTO shifts(guild_id,user_id,clock_in,hourly_rate_cents,work_type) VALUES(?,?,?,?,?)',
                       (interaction.guild_id, interaction.user.id, started, rate_cents,work_type))
            DB.commit()
            new_shift = (cursor.lastrowid, started)
            result = f'✅ Clocked in for **{work_type.title()}** at {money(rate_cents)}/hour.'
    await interaction.response.send_message(result, ephemeral=True)
    if new_shift:
        await post_clock_log(interaction, 'in', new_shift[0], new_shift[1])


async def clock_out(interaction):
    if not allowed_guild(interaction) or not employee(interaction):
        await interaction.response.send_message('You are not authorized to clock out here.', ephemeral=True)
        return
    completed_shift = None
    async with lock:
        shift = DB.execute('SELECT id,clock_in,hourly_rate_cents FROM shifts WHERE guild_id=? AND user_id=? AND clock_out IS NULL',
                           (interaction.guild_id, interaction.user.id)).fetchone()
        if not shift:
            paused = open_break(interaction.guild_id, interaction.user.id)
            if paused:
                ended = now_iso()
                DB.execute('UPDATE employee_breaks SET ended_at=?,ended_without_resuming=1 WHERE id=?', (ended,paused['id']))
                DB.commit()
                result = ('✅ Shift ended during your break. No additional paid hours were added. '
                          f'Unpaid break: {break_duration_text(paused["started_at"], ended)}.')
            else:
                result = 'You do not have an open shift.'
        else:
            ended = now_iso()
            DB.execute('UPDATE shifts SET clock_out=? WHERE id=?', (ended, shift['id']))
            DB.commit()
            hours = (datetime.fromisoformat(ended) - datetime.fromisoformat(shift['clock_in'])).total_seconds()/3600
            kind = DB.execute('SELECT work_type FROM shifts WHERE id=?',(shift['id'],)).fetchone()['work_type']
            next_step = ' Submit revenue with `/commission` if needed.' if kind!='packing' else ''
            result = f'✅ Clocked out **{kind.title()}**. Shift #{shift["id"]}: {hours:.2f} hours.' + next_step
            completed_shift = (shift['id'], shift['clock_in'], ended,
                               shift['hourly_rate_cents'] if shift['hourly_rate_cents'] is not None else HOURLY_RATE_CENTS)
    await interaction.response.send_message(result, ephemeral=True)
    if completed_shift:
        await post_clock_log(interaction, 'out', *completed_shift)


async def start_break(interaction):
    """Pause paid time and remember exact work type/rate for restart."""
    if not allowed_guild(interaction) or not employee(interaction):
        await interaction.response.send_message('Employee access required.',ephemeral=True)
        return
    completed_shift = None
    async with lock:
        if open_break(interaction.guild_id, interaction.user.id):
            message = 'You are already on break. Press **End Break / Resume** when ready.'
        else:
            shift=DB.execute("""SELECT * FROM shifts WHERE guild_id=? AND user_id=?
                AND clock_out IS NULL""",(interaction.guild_id,interaction.user.id)).fetchone()
            if not shift:
                message='You need to clock in before starting a break.'
            else:
                paused=now_iso()
                kind=shift['work_type'] or 'streaming'
                rate=int(shift['hourly_rate_cents'] if shift['hourly_rate_cents'] is not None else
                         shift_rate_for_employee(interaction.guild_id, interaction.user.id,kind))
                with DB:
                    DB.execute('UPDATE shifts SET clock_out=? WHERE id=?',(paused,shift['id']))
                    DB.execute("""INSERT INTO employee_breaks
                        (guild_id,user_id,prior_shift_id,work_type,hourly_rate_cents,started_at)
                        VALUES(?,?,?,?,?,?)""",
                        (interaction.guild_id,interaction.user.id,shift['id'],kind,rate,paused))
                completed_shift=(shift['id'],shift['clock_in'],paused,rate)
                message=f'🍽️ Break started. **Paid time is paused.** Your {kind.title()} shift will resume at {money(rate)}/hour when you press **End Break / Resume**.'
    await interaction.response.send_message(message,ephemeral=True)
    if completed_shift:
        await post_clock_log(interaction,'out',*completed_shift,event_type='break_start')


async def end_break(interaction):
    """Resume original work type and original rate as a new paid shift segment."""
    if not allowed_guild(interaction) or not employee(interaction):
        await interaction.response.send_message('Employee access required.',ephemeral=True)
        return
    new_shift=None
    async with lock:
        paused=open_break(interaction.guild_id, interaction.user.id)
        if not paused:
            message='You have no active break. Use **Clock In** if you need to start a shift.'
        elif DB.execute('SELECT id FROM shifts WHERE guild_id=? AND user_id=? AND clock_out IS NULL',
                        (interaction.guild_id,interaction.user.id)).fetchone():
            message='You are already clocked in. Ask a manager to review your break record.'
        else:
            resumed=now_iso()
            with DB:
                cur=DB.execute("""INSERT INTO shifts(guild_id,user_id,clock_in,hourly_rate_cents,work_type)
                    VALUES(?,?,?,?,?)""",
                    (interaction.guild_id,interaction.user.id,resumed,paused['hourly_rate_cents'],paused['work_type']))
                DB.execute('UPDATE employee_breaks SET ended_at=?,next_shift_id=? WHERE id=?',
                           (resumed,cur.lastrowid,paused['id']))
            new_shift=(cur.lastrowid,resumed)
            message=(f'✅ Back from break! **{paused["work_type"].title()}** resumed at '
                     f'{money(paused["hourly_rate_cents"])}/hour. '
                     f'Unpaid break: {break_duration_text(paused["started_at"],resumed)}.')
    await interaction.response.send_message(message,ephemeral=True)
    if new_shift:
        await post_clock_log(interaction,'in',*new_shift,event_type='break_resume')


# V32: Employee self-service switching. End the paid segment and start the new
# work type at the same exact UTC instant. Never overlap or duplicate base hours.
def switch_work_type_record(guild_id, user_id, target):
    if target not in ('streaming', 'packing'):
        raise ValueError('Choose Streaming or Packing.')
    with DB:
        if open_break(guild_id, user_id):
            raise ValueError('End your meal break before switching work types.')
        current = DB.execute('SELECT * FROM shifts WHERE guild_id=? AND user_id=? AND clock_out IS NULL',
                             (guild_id, user_id)).fetchone()
        if current is None:
            raise ValueError('Clock in first to switch work types.')
        if current['work_type'] == target:
            raise ValueError(f'Already clocked in as {target.title()}.')
        instant = now_iso()
        if datetime.fromisoformat(instant) <= datetime.fromisoformat(current['clock_in']):
            raise ValueError('Wait a moment before switching work types.')
        rate = shift_rate_for_employee(guild_id, user_id, target)
        DB.execute('UPDATE shifts SET clock_out=? WHERE id=? AND clock_out IS NULL', (instant, current['id']))
        new = DB.execute('INSERT INTO shifts(guild_id,user_id,clock_in,hourly_rate_cents,work_type) VALUES (?,?,?,?,?)',
                         (guild_id, user_id, instant, rate, target))
        return int(current['id']), int(new.lastrowid), instant, int(rate)


async def switch_work_type(interaction, target):
    if not allowed_guild(interaction) or not employee(interaction):
        return await interaction.response.send_message('Employee access required.', ephemeral=True)
    async with lock:
        try:
            old_id, new_id, stamp, rate = switch_work_type_record(interaction.guild_id, interaction.user.id, target)
        except (ValueError, sqlite3.Error) as exc:
            return await interaction.response.send_message(str(exc), ephemeral=True)
    await interaction.response.send_message(
        f'✅ Switched to **{target.title()}** at **{money(rate)}/hr**. Shift #{old_id} closed; shift #{new_id} started. '
        'Your prior paid hours are preserved.', ephemeral=True)
    old = DB.execute('SELECT clock_in,hourly_rate_cents FROM shifts WHERE id=? AND guild_id=?',
                     (old_id,interaction.guild_id)).fetchone()
    if old:
        await post_clock_log(interaction, 'out', old_id, old['clock_in'], stamp, old['hourly_rate_cents'],event_type='switch_out')
    await post_clock_log(interaction, 'in', new_id, stamp,event_type='switch_in')


class WorkSwitchChooser(discord.ui.View):
    def __init__(self, owner_id):
        super().__init__(timeout=180)
        self.owner_id = owner_id

    async def interaction_check(self, interaction):
        if interaction.user.id != self.owner_id or not allowed_guild(interaction) or not employee(interaction):
            await interaction.response.send_message('Open your own switch menu.', ephemeral=True)
            return False
        return True

    @discord.ui.button(label='Switch to Streaming', style=discord.ButtonStyle.primary)
    async def streaming(self, interaction, button):
        await switch_work_type(interaction, 'streaming')

    @discord.ui.button(label='Switch to Packing', style=discord.ButtonStyle.secondary)
    async def packing(self, interaction, button):
        await switch_work_type(interaction, 'packing')


class WorkTypeChooser(discord.ui.View):
    def __init__(self, owner_id):
        super().__init__(timeout=180)
        self.owner_id=owner_id

    async def interaction_check(self,interaction):
        if interaction.user.id != self.owner_id or not allowed_guild(interaction) or not employee(interaction):
            await interaction.response.send_message('Open your own clock-in menu.',ephemeral=True)
            return False
        return True

    @discord.ui.button(label='Streaming · employee rate + commission',style=discord.ButtonStyle.primary)
    async def streaming(self,interaction,button):
        await clock_in(interaction,'streaming')

    @discord.ui.button(label='Packing · employee rate',style=discord.ButtonStyle.secondary)
    async def packing(self,interaction,button):
        await clock_in(interaction,'packing')


class TimeClock(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Clock In', style=discord.ButtonStyle.success, emoji='🟢', custom_id='etbeast_demo:clockin')
    async def cin(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message('What are you working on?',view=WorkTypeChooser(interaction.user.id),ephemeral=True)

    @discord.ui.button(label='Clock Out', style=discord.ButtonStyle.danger, emoji='🔴', custom_id='etbeast_demo:clockout')
    async def cout(self, interaction: discord.Interaction, button: discord.ui.Button):
        await clock_out(interaction)

    @discord.ui.button(label='Switch to Streaming',style=discord.ButtonStyle.primary,emoji='🔁',custom_id='etbeast_demo:switchtostream',row=1)
    async def switch_stream(self,interaction:discord.Interaction,button:discord.ui.Button):
        await switch_work_type(interaction,'streaming')

    @discord.ui.button(label='Switch to Packing',style=discord.ButtonStyle.primary,emoji='📦',custom_id='etbeast_demo:switchtopack',row=1)
    async def switch_pack(self,interaction:discord.Interaction,button:discord.ui.Button):
        await switch_work_type(interaction,'packing')

    @discord.ui.button(label='Start Break',style=discord.ButtonStyle.secondary,emoji='🍽️',custom_id='etbeast_demo:breakstart')
    async def bstart(self,interaction: discord.Interaction, button: discord.ui.Button):
        await start_break(interaction)

    @discord.ui.button(label='End Break / Resume',style=discord.ButtonStyle.primary,emoji='▶️',custom_id='etbeast_demo:breakend')
    async def bend(self,interaction: discord.Interaction, button: discord.ui.Button):
        await end_break(interaction)


class CommissionReview(discord.ui.View):
    def __init__(self, submission_id: int):
        super().__init__(timeout=None)
        self.submission_id = submission_id
        for child in self.children:
            child.custom_id = f'etbeast_demo:review:{submission_id}:{"approve" if child.label == "Approve" else "reject"}'

    async def resolve(self, interaction, status):
        if not allowed_guild(interaction) or not manager(interaction):
            await interaction.response.send_message('Manager permission required.', ephemeral=True)
            return
        # Acknowledge the button BEFORE any SQLite or Discord network work.
        # This protects approvals from Discord's short interaction timeout.
        await interaction.response.defer(ephemeral=True, thinking=True)
        async with lock:
            entry = DB.execute('SELECT * FROM commissions WHERE id=? AND guild_id=?',
                               (self.submission_id, interaction.guild_id)).fetchone()
            if entry and entry['user_id'] == interaction.user.id:
                await interaction.followup.send('You cannot review your own commission submission.', ephemeral=True)
                return
            if not entry:
                await interaction.followup.send(f'Commission #{self.submission_id} is unavailable or was removed.', ephemeral=True)
                return
            if entry['status'] != 'pending':
                actual = str(entry['status']).replace('_', ' ').title()
                await interaction.followup.send(
                    f'Commission #{self.submission_id} is already **{actual}**. No additional change was made. '
                    'Refresh the manager dashboard to see the current pending count.', ephemeral=True)
                # Best effort: clear stale controls without making the response fail.
                await self._disable_review_buttons(interaction)
                return
            if status == 'approved' and entry['stream_type'] == 'character_break':
                if entry['character_hours'] is None or (entry['sales_per_hour_cents'] is None and (entry['pack_size'] is None or entry['break_count'] is None)):
                    await interaction.followup.send('Character Break record missing duration verification data; cannot approve.', ephemeral=True)
                    return
                associated = DB.execute('SELECT * FROM shifts WHERE id=? AND guild_id=?',
                                        (entry['shift_id'], interaction.guild_id)).fetchone()
                if not associated or associated['clock_out'] is None:
                    await interaction.followup.send('Close the Streaming shift before approving a Character Break guarantee.', ephemeral=True)
                    return
                # Flat Character Break credits need not equal actual shift duration.
            designated_week = assigned_commission_week(interaction.guild_id, dict(entry, clock_out=(
                DB.execute('SELECT clock_out FROM shifts WHERE id=?', (entry['shift_id'],)).fetchone()['clock_out']
            ))) if status == 'approved' else None
            if status == 'approved' and payroll_week_status(interaction.guild_id, designated_week):
                await interaction.followup.send(
                    f'Commission #{self.submission_id} belongs to already-approved week {designated_week}. '
                    'It was NOT approved or moved into another week; reconcile it with payroll management first.',
                    ephemeral=True)
                return
            DB.execute('UPDATE commissions SET status=?, reviewed_by=?, reviewed_at=?, payroll_week_start=? WHERE id=? AND status=?',
                       (status, interaction.user.id, now_iso(), designated_week, self.submission_id, 'pending'))
            DB.commit()
        # The DB commit is authoritative. Never imply an approval failed because
        # updating its public review message failed afterwards.
        await interaction.followup.send(
            f'✅ Commission #{self.submission_id} **{status}** successfully and saved to payroll. '
            'The pending-review count updates on a fresh dashboard.', ephemeral=True)
        await self._disable_review_buttons(interaction)

    async def _disable_review_buttons(self, interaction):
        for item in self.children:
            item.disabled = True
        try:
            if interaction.message:
                await interaction.message.edit(view=self)
        except (discord.HTTPException, discord.NotFound, discord.Forbidden) as exc:
            # A stale/deleted/inaccessible review message must never undo or
            # misrepresent the already-committed payroll decision.
            print(f'Commission #{self.submission_id}: review message refresh failed: {exc!r}')

    @discord.ui.button(label='Approve', style=discord.ButtonStyle.success)
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.resolve(interaction, 'approved')

    @discord.ui.button(label='Reject', style=discord.ButtonStyle.danger)
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.resolve(interaction, 'rejected')


# V41: Pending commissions are always available from SQLite, even if their
# original Discord review message is missing or was deleted.
def pending_review_rows(guild_id):
    return DB.execute("SELECT id, user_id, commission_cents, guarantee_topup_cents, stream_type "
                      "FROM commissions WHERE guild_id=? AND status='pending' ORDER BY id ASC",
                      (guild_id,)).fetchall()


def commission_proof_paths(commission_id):
    paths = [r['attachment_path'] for r in DB.execute(
        'SELECT attachment_path FROM commission_attachments WHERE commission_id=? ORDER BY ordinal',
        (commission_id,))]
    if not paths:
        row = DB.execute('SELECT attachment_path FROM commissions WHERE id=?', (commission_id,)).fetchone()
        if row and row['attachment_path'] not in (None, '', 'pending-upload'):
            paths = [row['attachment_path']]
    # Never attach arbitrary filesystem paths stored in a database row.
    root = UPLOAD_DIR.resolve()
    valid = []
    for raw in paths:
        path = Path(raw).resolve()
        if path.is_relative_to(root) and path.is_file():
            valid.append(path)
    return valid


async def show_commission_for_review(interaction, submission_id):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.', ephemeral=True)
    await interaction.response.defer(ephemeral=True, thinking=True)
    entry = DB.execute('SELECT * FROM commissions WHERE guild_id=? AND id=?',
                       (interaction.guild_id, submission_id)).fetchone()
    if not entry:
        return await interaction.followup.send(f'Commission #{submission_id} was not found in this server.', ephemeral=True)
    if entry['status'] != 'pending':
        return await interaction.followup.send(
            f'Commission #{submission_id} is already **{entry["status"]}**. No approval action is available.',
            ephemeral=True)
    embed = discord.Embed(title=f'Commission #{submission_id} — Pending Review', color=discord.Color.orange())
    member = interaction.guild.get_member(entry['user_id']) if interaction.guild else None
    embed.add_field(name='Employee', value=(member.display_name if member else f'ID {entry["user_id"]}'), inline=True)
    embed.add_field(name='Streaming shift', value=f'#{entry["shift_id"]}', inline=True)
    embed.add_field(name='Type', value=str(entry['stream_type']).replace('_', ' ').title(), inline=True)
    embed.add_field(name='Revenue', value=money(entry['revenue_cents'] or 0), inline=True)
    embed.add_field(name='Commission', value=money(entry['commission_cents']), inline=True)
    embed.add_field(name='Character Break top-up', value=money(entry['guarantee_topup_cents'] or 0), inline=True)
    if entry['pack_size']:
        embed.add_field(name='Character Break packs', value=f'{entry["pack_size"]}-pack × {entry["break_count"] or 1}', inline=True)
    embed.add_field(name='Review', value='Verify original proof and break completion before approving. The database is the source of truth.', inline=False)
    paths = commission_proof_paths(submission_id)
    if not paths:
        embed.add_field(name='⚠️ Missing proof', value='No accessible screenshot files found. Do not approve until verified.', inline=False)
    elif len(paths) > 10:
        paths = paths[:10]
    # A private, temporary review view avoids reliance on the original public post.
    with ExitStack() as stack:
        files = [discord.File(stack.enter_context(open(path, 'rb')), filename=f'proof{i}{path.suffix}')
                 for i, path in enumerate(paths, 1)]
        review_view = CommissionReview(submission_id)
        if not paths:
            for control in review_view.children:
                if isinstance(control, discord.ui.Button) and control.label == 'Approve':
                    control.disabled = True
        await interaction.followup.send(embed=embed, files=files,
                                        view=review_view, ephemeral=True)


class V41CommissionIdModal(discord.ui.Modal, title='Review commission by ID'):
    submission_id = discord.ui.TextInput(label='Commission number', placeholder='144', min_length=1, max_length=12)

    async def on_submit(self, interaction):
        value = self.submission_id.value.strip().lstrip('#')
        if not value.isdecimal() or int(value) <= 0:
            return await interaction.response.send_message('Enter a valid positive commission ID.', ephemeral=True)
        await show_commission_for_review(interaction, int(value))


class V41PendingReviewList(discord.ui.View):
    PAGE_SIZE = 20

    def __init__(self, owner_id, guild_id, page=0):
        super().__init__(timeout=300)
        self.owner_id = owner_id
        self.guild_id = guild_id
        rows = pending_review_rows(guild_id)
        last_page = max(0, (len(rows) - 1) // self.PAGE_SIZE)
        self.page = max(0, min(page, last_page))
        subset = rows[self.page * self.PAGE_SIZE:(self.page + 1) * self.PAGE_SIZE]
        if subset:
            select = discord.ui.Select(placeholder='Select any pending commission', options=[
                discord.SelectOption(label=f'#{r["id"]} — {money((r["commission_cents"] or 0)+(r["guarantee_topup_cents"] or 0))}',
                                     description=f'Employee ID {r["user_id"]} · {r["stream_type"]}'[:100], value=str(r['id']))
                for r in subset])
            async def selected(interaction):
                if not self._authorized(interaction):
                    return await interaction.response.send_message('Manager access required.', ephemeral=True)
                await show_commission_for_review(interaction, int(select.values[0]))
            select.callback = selected
            self.add_item(select)
        self.earlier.disabled = self.page == 0
        self.later.disabled = self.page >= last_page

    def _authorized(self, interaction):
        return (interaction.user.id == self.owner_id and interaction.guild_id == self.guild_id
                and allowed_guild(interaction) and manager(interaction))

    @discord.ui.button(label='Previous', style=discord.ButtonStyle.secondary, row=1)
    async def earlier(self, interaction, button):
        if not self._authorized(interaction):
            return await interaction.response.send_message('Manager access required.', ephemeral=True)
        await interaction.response.edit_message(content=pending_review_summary(self.guild_id, self.page-1),
                                                view=V41PendingReviewList(self.owner_id,self.guild_id,self.page-1))

    @discord.ui.button(label='Next', style=discord.ButtonStyle.secondary, row=1)
    async def later(self, interaction, button):
        if not self._authorized(interaction):
            return await interaction.response.send_message('Manager access required.', ephemeral=True)
        await interaction.response.edit_message(content=pending_review_summary(self.guild_id, self.page+1),
                                                view=V41PendingReviewList(self.owner_id,self.guild_id,self.page+1))

    @discord.ui.button(label='Refresh', style=discord.ButtonStyle.primary, row=1)
    async def refresh(self, interaction, button):
        if not self._authorized(interaction):
            return await interaction.response.send_message('Manager access required.', ephemeral=True)
        await interaction.response.edit_message(content=pending_review_summary(self.guild_id,self.page),
                                                view=V41PendingReviewList(self.owner_id,self.guild_id,self.page))

    @discord.ui.button(label='Review by ID', style=discord.ButtonStyle.success, row=2)
    async def by_id(self, interaction, button):
        if not self._authorized(interaction):
            return await interaction.response.send_message('Manager access required.', ephemeral=True)
        await interaction.response.send_modal(V41CommissionIdModal())


def pending_review_summary(guild_id, page=0):
    rows = pending_review_rows(guild_id)
    last_page = max(0,(len(rows)-1)//V41PendingReviewList.PAGE_SIZE)
    page = max(0,min(page,last_page))
    return (f'**Pending commissions: {len(rows)}** · Page {page+1}/{last_page+1}\n'
            'Read directly from SQLite. Refresh after reviewing; use **Review by ID** for a missing message.')


class ETBeastDemo(commands.Bot):
    async def setup_hook(self):
        self.add_view(TimeClock())
        for entry in DB.execute('SELECT id FROM commissions WHERE status="pending" AND review_message_id IS NOT NULL'):
            self.add_view(CommissionReview(entry['id']))
        for row in DB.execute("SELECT id FROM expense_submissions WHERE status='pending' AND review_message_id IS NOT NULL"):
            self.add_view(ExpenseReview(row['id']))
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()


# Member intent is needed to resolve saved user IDs to current server nicknames.
# Enable Server Members Intent in the Discord Developer Portal > Bot, too.
intents = discord.Intents.default()
intents.members = True
bot = ETBeastDemo(command_prefix='!', intents=intents)


@tasks.loop(minutes=30)
async def stale_shift_watch():
    # A warning is advisory. It NEVER clocks anyone out or changes their wages.
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat()
    active = DB.execute('SELECT id,guild_id,user_id,clock_in FROM shifts WHERE clock_out IS NULL AND clock_in<?', (cutoff,)).fetchall()
    for shift in active:
        guild = bot.get_guild(shift['guild_id'])
        if not guild:
            continue
        channel_id = settings_for(guild.id)['log_channel_id']
        channel = guild.get_channel(channel_id) if channel_id else None
        if not isinstance(channel, discord.TextChannel):
            continue
        if DB.execute('SELECT 1 FROM shift_alerts WHERE guild_id=? AND shift_id=? AND alert_kind=?',
                      (guild.id,shift['id'],'12h')).fetchone():
            continue
        try:
            await channel.send(f'⚠️ **Possible forgotten clock-out:** {await resolve_employee_name(guild,shift["user_id"])} has an open shift '
                               f'#{shift["id"]} since <t:{int(datetime.fromisoformat(shift["clock_in"]).timestamp())}:F>. '
                               'Verify actual hours; use `/editshift` if a correction is required.',
                               allowed_mentions=discord.AllowedMentions.none())
            DB.execute('INSERT OR IGNORE INTO shift_alerts VALUES (?,?,?,?)',
                       (guild.id,shift['id'],'12h',now_iso()))
            DB.commit()
        except (discord.HTTPException,discord.Forbidden):
            logging.exception('Could not alert for shift #%s',shift['id'])

# V32: one reminder per planned shift, sent after the planned start if the
# employee has not clocked in. A DM failure never changes payroll or scheduling.
DB.execute("""CREATE TABLE IF NOT EXISTS schedule_clockin_reminders (
    planned_shift_id INTEGER PRIMARY KEY, notified_at TEXT NOT NULL, channel TEXT NOT NULL
)""")
DB.commit()


def planned_shift_start_utc(row):
    local=datetime.fromisoformat(f'{row["work_date"]}T{row["start_local"]}')
    return local.replace(tzinfo=SCHEDULE_TZ).astimezone(timezone.utc)


def late_planned_shifts(now=None):
    current=now or datetime.now(timezone.utc)
    start_range=(current-timedelta(hours=2)).date().isoformat()
    end_range=(current+timedelta(days=1)).date().isoformat()
    rows=DB.execute("""SELECT p.* FROM planned_shifts p
      LEFT JOIN schedule_clockin_reminders n ON n.planned_shift_id=p.id
      WHERE p.cancelled_at IS NULL AND n.planned_shift_id IS NULL
      AND p.work_date>=? AND p.work_date<=?""",(start_range,end_range)).fetchall()
    missing=[]
    for row in rows:
        due=planned_shift_start_utc(row)+timedelta(minutes=10)
        if current<due or current>due+timedelta(hours=2):
            continue
        # A shift clocked in around the scheduled start qualifies; ignore work type.
        early=(planned_shift_start_utc(row)-timedelta(minutes=30)).isoformat()
        present=DB.execute('SELECT 1 FROM shifts WHERE guild_id=? AND user_id=? AND clock_in<=? AND (clock_out IS NULL OR clock_out>=?) LIMIT 1',
           (row['guild_id'],row['user_id'],current.isoformat(),early)).fetchone()
        if not present:
            missing.append(row)
    return missing


@tasks.loop(minutes=10)
async def scheduled_clockin_watch():
    for entry in late_planned_shifts():
        guild=bot.get_guild(entry['guild_id'])
        if not guild: continue
        member=guild.get_member(entry['user_id'])
        if member is None:
            try:member=await guild.fetch_member(entry['user_id'])
            except (discord.NotFound,discord.Forbidden,discord.HTTPException):continue
        try:
            await member.send(f'⏰ ETBeastDemo reminder: your scheduled shift began at **{entry["start_local"]} ET**. '
                              'If you are working, please clock in. If you are absent, contact management.')
            channel='dm'
        except (discord.Forbidden,discord.HTTPException):
            channel='unavailable'
        # Deduplicate even if DMs are blocked; do not spam employees.
        with DB:
            DB.execute('INSERT OR IGNORE INTO schedule_clockin_reminders(planned_shift_id,notified_at,channel) VALUES(?,?,?)',
                       (entry['id'],now_iso(),channel))


@scheduled_clockin_watch.before_loop
async def before_clockin_watch():
    await bot.wait_until_ready()


@stale_shift_watch.before_loop
async def before_shift_watch():
    await bot.wait_until_ready()

# V36: retain the original approval and full reason when a manager voids a mistake.
for col, definition in {
    'voided_at': 'TEXT', 'voided_by': 'INTEGER', 'void_reason': 'TEXT',
}.items():
    if col not in {r['name'] for r in DB.execute('PRAGMA table_info(commissions)')}:
        DB.execute(f'ALTER TABLE commissions ADD COLUMN {col} {definition}')
DB.commit()

# V34 commission review status transition audit; captures reviewer and old/new status.
DB.executescript("""
CREATE TABLE IF NOT EXISTS commission_review_audit (
 id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
 commission_id INTEGER NOT NULL, employee_id INTEGER NOT NULL,
 actor_id INTEGER, occurred_at TEXT NOT NULL, old_status TEXT NOT NULL,
 new_status TEXT NOT NULL, previous_week TEXT, new_week TEXT
);
DROP TRIGGER IF EXISTS v34_commission_review_changes;
CREATE TRIGGER v34_commission_review_changes
AFTER UPDATE OF status ON commissions
WHEN OLD.status != NEW.status
BEGIN
 INSERT INTO commission_review_audit(guild_id,commission_id,employee_id,actor_id,
   occurred_at,old_status,new_status,previous_week,new_week)
 VALUES(NEW.guild_id,NEW.id,NEW.user_id,
   CASE WHEN NEW.status='voided' THEN NEW.voided_by ELSE NEW.reviewed_by END,
   strftime('%Y-%m-%dT%H:%M:%fZ','now'),OLD.status,NEW.status,
   OLD.payroll_week_start,NEW.payroll_week_start);
END;
""")
DB.commit()

# V34: Daily restorable, consistent SQLite + proof backups. No live files are modified.
# Set ETBEAST_DEMO_BACKUP_DIR to a secured *off-data-directory* destination if desired.
BACKUP_DIR = Path(os.getenv('ETBEAST_DEMO_BACKUP_DIR', str(DATA_DIR.resolve().parent / 'etbeast-demo-backups'))).expanduser().resolve()
BACKUP_KEEP_DAYS = 30


def create_daily_backup(day=None):
    day = day or datetime.now(timezone.utc).date()
    # Keep backup archives OUTSIDE data/ so archives can never back themselves up.
    if BACKUP_DIR == DATA_DIR.resolve() or DATA_DIR.resolve() in BACKUP_DIR.parents:
        raise ValueError('Backup directory cannot be inside the live data directory')
    BACKUP_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name == 'posix':
        os.chmod(BACKUP_DIR, 0o700)
    target = BACKUP_DIR / f'etbeast_demo-{day.isoformat()}.tar.gz'
    if target.exists():
        return target
    with tempfile.TemporaryDirectory(prefix='hh-snapshot-', dir=str(BACKUP_DIR)) as temp:
        tmp = Path(temp)
        db_snapshot = tmp / 'etbeast_demo.sqlite3'
        # sqlite backup() copies a consistent point-in-time snapshot, including WAL changes.
        with sqlite3.connect(db_snapshot) as destination:
            DB.backup(destination, pages=100, sleep=0.05)
        with sqlite3.connect(db_snapshot) as verify:
            result = verify.execute('PRAGMA integrity_check').fetchone()[0]
            if result != 'ok':
                raise RuntimeError(f'SQLite backup integrity check failed: {result}')
        manifest = {'created_at_utc': datetime.now(timezone.utc).isoformat(),
                    'sqlite_file': 'etbeast_demo.sqlite3', 'proof_directory': 'proofs',
                    'note': 'Restore only with bot stopped; proof files captured during archive creation.'}
        (tmp / 'manifest.json').write_text(json.dumps(manifest, indent=2))
        archive_tmp = tmp / 'staging.tar.gz'
        with tarfile.open(archive_tmp, 'w:gz') as archive:
            archive.add(db_snapshot, arcname='etbeast_demo.sqlite3')
            archive.add(tmp / 'manifest.json', arcname='manifest.json')
            if UPLOAD_DIR.is_dir():
                archive.add(UPLOAD_DIR, arcname='proofs', recursive=True)
        with tarfile.open(archive_tmp, 'r:gz') as archive:
            if not {'etbeast_demo.sqlite3', 'manifest.json'}.issubset(set(archive.getnames())):
                raise RuntimeError('Backup archive is missing required files')
        staging = BACKUP_DIR / (target.name + '.partial')
        try:
            shutil.copyfile(archive_tmp, staging)
            if os.name == 'posix':
                os.chmod(staging, 0o600)
            staging.replace(target)
        finally:
            staging.unlink(missing_ok=True)
    expiry = day - timedelta(days=BACKUP_KEEP_DAYS)
    for old in BACKUP_DIR.glob('etbeast_demo-????-??-??.tar.gz'):
        try:
            when = date.fromisoformat(old.name[10:20])
        except ValueError:
            continue
        if when < expiry:
            old.unlink()
    logging.info('V34 daily backup completed: %s', target)
    return target


@tasks.loop(hours=1)
async def daily_backup_watch():
    try:
        await asyncio.to_thread(create_daily_backup)
    except Exception:
        logging.exception('V34 daily payroll/proof backup FAILED; fix storage/permissions')


@daily_backup_watch.before_loop
async def before_daily_backup_watch():
    await bot.wait_until_ready()


def manager_audit_rows(guild_id):
    """Consolidate actor-attributed history already tracked across v6-v33 ledgers."""
    events=[]
    sources=[
      ('shift_audit', 'created_at', 'actor_id', 'action', 'shift_id', 'reason'),
      ('settings_audit', 'changed_at', 'actor_id', 'setting', 'id', 'new_value'),
      ('schedule_audit', 'created_at', 'actor_id', 'action', 'id', 'detail'),
      ('payroll_period_audit', 'occurred_at', 'actor_id', 'action', 'week_start', 'details'),
      ('incident_fine_audit', 'occurred_at', 'actor_id', 'action', 'incident_id', 'note'),
    ]
    for table, clock, actor, action, record, detail in sources:
        for r in DB.execute(f'SELECT {clock} stamp,{actor} actor,{action} action,{record} record,{detail} detail FROM {table} WHERE guild_id=?', (guild_id,)):
            events.append((r['stamp'], table, r['actor'], r['action'], r['record'], r['detail']))
    for r in DB.execute('SELECT * FROM bonuses WHERE guild_id=?', (guild_id,)):
        events.append((r['created_at'], 'bonuses', r['created_by'], 'created', r['id'], f"employee {r['employee_id']}; {money(r['amount_cents'])}; {r['reason']}"))
        if r['voided_at']:
            events.append((r['voided_at'], 'bonuses', r['voided_by'], 'voided', r['id'], r['void_reason']))
    for r in DB.execute('SELECT * FROM employee_payments WHERE guild_id=?', (guild_id,)):
        events.append((r['recorded_at'], 'payments', r['recorded_by'], 'recorded', r['id'], f"employee {r['employee_id']}; {money(r['amount_cents'])}; {r['method']}; {r['week_start']}"))
        if r['voided_at']:
            events.append((r['voided_at'], 'payments', r['voided_by'], 'voided', r['id'], r['void_reason']))
    for r in DB.execute('SELECT * FROM commission_review_audit WHERE guild_id=?', (guild_id,)):
        void_note = ''
        if r['new_status'] == 'voided':
            void_row = DB.execute('SELECT void_reason FROM commissions WHERE guild_id=? AND id=?',
                                  (guild_id,r['commission_id'])).fetchone()
            void_note = f"; void reason: {void_row['void_reason']}" if void_row and void_row['void_reason'] else ''
        events.append((r['occurred_at'], 'commission_review', r['actor_id'],
            f"{r['old_status']} → {r['new_status']}", r['commission_id'],
            f"employee {r['employee_id']}; payroll {r['previous_week']} → {r['new_week']}{void_note}"))
    events.sort(key=lambda row: str(row[0]), reverse=True)
    return events


def void_approved_commission(guild_id, commission_id, actor_id, reason):
    """Void, never erase, a commission from an open pay period.

    Call under the bot's asyncio lock. The caller provides a manager-verified actor.
    Returns the old record for the confirmation message.
    """
    reason = str(reason).strip()
    if not reason or len(reason) > 300:
        raise ValueError('Provide a correction reason (1–300 characters).')
    with DB:
        record = DB.execute('SELECT * FROM commissions WHERE guild_id=? AND id=?',
                            (guild_id, commission_id)).fetchone()
        if not record:
            raise ValueError('Commission submission not found in this server.')
        if record['status'] != 'approved':
            raise ValueError('Only approved commissions can be voided; this one is '+str(record['status'])+'.')
        # A previously finalized or paid week's approved earning must not be silently modified.
        week = record['payroll_week_start']
        if not week:
            shift = DB.execute('SELECT clock_out FROM shifts WHERE id=? AND guild_id=?',
                               (record['shift_id'], guild_id)).fetchone()
            week = assigned_commission_week(guild_id, dict(record, clock_out=(shift['clock_out'] if shift else None)))
        if payroll_week_status(guild_id, week) is not None:
            raise ValueError(f'Payroll week {week} is already approved or paid. A formal payroll adjustment is required.')
        stamp = now_iso()
        changed = DB.execute("UPDATE commissions SET status='voided', voided_at=?, voided_by=?, void_reason=? WHERE guild_id=? AND id=? AND status='approved'",
          (stamp, actor_id, reason, guild_id, commission_id)).rowcount
        if changed != 1:
            raise ValueError('The submission changed while processing; try again.')
    return dict(record)


class CommissionVoidModal(discord.ui.Modal, title='Void approved commission'):
    reason = discord.ui.TextInput(label='Reason for correction',
                                  placeholder='Wrong screenshot, amount, break count, etc.',
                                  style=discord.TextStyle.paragraph, min_length=1, max_length=300)

    def __init__(self, owner, submission_id):
        super().__init__()
        self.owner = owner
        self.submission_id = submission_id

    async def on_submit(self, interaction):
        if (not allowed_guild(interaction) or not manager(interaction)
                or interaction.user.id != self.owner):
            return await interaction.response.send_message('Manager permission required.', ephemeral=True)
        async with lock:
            try:
                previous = void_approved_commission(interaction.guild_id, self.submission_id,
                                                    interaction.user.id, self.reason.value)
            except ValueError as exc:
                return await interaction.response.send_message(str(exc), ephemeral=True)
        await interaction.response.send_message(
            f'✅ Commission #{self.submission_id} voided. Removed from unpaid payroll: '
            f'{money(previous["commission_cents"])} commission + '
            f'{money(previous["guarantee_topup_cents"])} Character Break top-up. '
            'The original proof and approval history remain. '
            'Have the employee submit the corrected commission for review.', ephemeral=True)


class CommissionVoidPicker(discord.ui.View):
    def __init__(self, owner, entries):
        super().__init__(timeout=300)
        self.owner = owner
        options = [discord.SelectOption(
            label=f'#{r["id"]} · {money(r["commission_cents"] + r["guarantee_topup_cents"])}',
            description=f'Employee {r["user_id"]} · {r["stream_type"]}'[:100],
            value=str(r['id'])) for r in entries[:25]]
        select = discord.ui.Select(placeholder='Choose approved submission to void', options=options)
        async def select_one(interaction):
            if (not allowed_guild(interaction) or not manager(interaction)
                    or interaction.user.id != self.owner):
                return await interaction.response.send_message('Manager-only.', ephemeral=True)
            await interaction.response.send_modal(CommissionVoidModal(self.owner, int(select.values[0])))
        select.callback = select_one
        self.add_item(select)


@bot.tree.command(name='reviewcommission', description='Manager: review any pending commission by its ID, even if Discord post is missing')
@app_commands.guild_only()
@app_commands.describe(submission_id='Exact commission ID, e.g. 144')
async def reviewcommission(interaction: discord.Interaction, submission_id: int):
    await show_commission_for_review(interaction, submission_id)


@bot.tree.command(name='pendingcommissions', description='Manager: list all pending commissions with pagination and refresh')
@app_commands.guild_only()
async def pendingcommissions(interaction: discord.Interaction):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.', ephemeral=True)
    await interaction.response.send_message(pending_review_summary(interaction.guild_id),
                                            view=V41PendingReviewList(interaction.user.id, interaction.guild_id),
                                            ephemeral=True)


@bot.tree.command(name='voidcommission', description='Manager: void a mistaken approved commission in unfinalized payroll')
@app_commands.describe(submission_id='Approved commission submission ID', reason='Required correction reason')
@app_commands.guild_only()
async def voidcommission(interaction: discord.Interaction, submission_id: int, reason: str):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.', ephemeral=True)
    async with lock:
        try:
            previous = void_approved_commission(interaction.guild_id, submission_id,
                                                interaction.user.id, reason)
        except ValueError as exc:
            return await interaction.response.send_message(str(exc), ephemeral=True)
    await interaction.response.send_message(
        f'✅ Commission #{submission_id} voided. Removed from unpaid payroll: '
        f'{money(previous["commission_cents"])} commission + '
        f'{money(previous["guarantee_topup_cents"])} top-up. '
        'Original record and screenshots were preserved. Have the employee resubmit the correct information.',
        ephemeral=True)


@bot.tree.command(name='manageraudit', description='Manager: download consolidated historical changes CSV')
@app_commands.guild_only()
async def manageraudit(interaction: discord.Interaction):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.', ephemeral=True)
    rows = manager_audit_rows(interaction.guild_id)
    output=io.StringIO()
    writer=csv.writer(output)
    writer.writerow(['timestamp_utc', 'source', 'actor_discord_id', 'action', 'record_id', 'details'])
    writer.writerows(rows)
    data=output.getvalue().encode('utf-8-sig')
    if len(data)>23*1024*1024:
        return await interaction.response.send_message('Audit export is too large for Discord. Export directly from the server.', ephemeral=True)
    await interaction.response.send_message(f'{len(rows)} historical audited events. Manager-only download. Commission review changes are recorded beginning with v34; older decisions may have no individual history.',
        file=discord.File(io.BytesIO(data),filename='etbeast_demo_manager_audit.csv'),ephemeral=True)


@bot.tree.command(name='backupstatus', description='Manager: verify recent automated server backup')
@app_commands.guild_only()
async def backupstatus(interaction:discord.Interaction):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.', ephemeral=True)
    recent=sorted(BACKUP_DIR.glob('etbeast_demo-????-??-??.tar.gz'), reverse=True) if BACKUP_DIR.is_dir() else []
    if not recent:
        return await interaction.response.send_message('⚠️ No completed automatic backups found. Check service logs and backup directory permissions.', ephemeral=True)
    latest=recent[0]
    age=datetime.now(timezone.utc)-datetime.fromtimestamp(latest.stat().st_mtime, timezone.utc)
    label='✅' if age < timedelta(hours=36) else '⚠️ Stale backup:'
    await interaction.response.send_message(f'{label} Latest backup: `{latest.name}` ({latest.stat().st_size/1024/1024:.1f} MiB, {age.total_seconds()/3600:.1f} hours old).\nStored on this server in `{BACKUP_DIR}`. Keep a separate off-server encrypted copy.',ephemeral=True)


@bot.event
async def on_ready():
    logging.info('ETBeastDemo connected as %s', bot.user)
    if not stale_shift_watch.is_running():
        stale_shift_watch.start()
    if not scheduled_clockin_watch.is_running():
        scheduled_clockin_watch.start()
    if not employee_log_retry_watch.is_running():
        employee_log_retry_watch.start()
    if not weekly_schedule_refresh.is_running():
        weekly_schedule_refresh.start()
    if not daily_backup_watch.is_running():
        daily_backup_watch.start()


@bot.tree.command(name='settings', description='Manager: configure pay and the commission review channel')
@app_commands.guild_only()
async def settings_command(interaction: discord.Interaction):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    await interaction.response.send_message(embed=configuration_embed(interaction.guild_id),
                                            view=SettingsView(), ephemeral=True)


@bot.tree.command(name='resetpay', description='Manager: remove an employee pay override')
@app_commands.guild_only()
async def resetpay(interaction: discord.Interaction, employee: discord.Member):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    async with lock:
        old = DB.execute('SELECT * FROM employee_rates WHERE guild_id=? AND user_id=?',
                         (interaction.guild_id, employee.id)).fetchone()
        if old:
            DB.execute('DELETE FROM employee_rates WHERE guild_id=? AND user_id=?',
                       (interaction.guild_id, employee.id))
            audit_setting(interaction.guild_id, interaction.user.id, f'employee:{employee.id}',
                          dict(old), 'default')
            DB.commit()
    await interaction.response.send_message(f'✅ {employee.mention} now uses default pay rates for future shifts and submissions.', ephemeral=True)


@bot.tree.command(name='switchwork', description='Switch between Streaming and Packing without losing clocked hours')
@app_commands.guild_only()
@app_commands.choices(work=[app_commands.Choice(name='Streaming',value='streaming'),
                            app_commands.Choice(name='Packing',value='packing')])
async def switchwork_command(interaction: discord.Interaction, work: app_commands.Choice[str]):
    await switch_work_type(interaction, work.value)


@bot.tree.command(name='setupclock', description='Manager: post permanent clock-in/out buttons')
@app_commands.guild_only()
async def setupclock(interaction: discord.Interaction):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    await interaction.response.send_message('🕒 **ETBeastDemo Time Clock**\nClock in, switch between Packing and Streaming, start/end unpaid meal breaks, or clock out. Your actions are privately confirmed.', view=TimeClock())


@bot.tree.command(name='clockin', description='Clock in for Streaming or Packing')
@app_commands.guild_only()
@app_commands.describe(work='Choose the type of work for this shift')
@app_commands.choices(work=[app_commands.Choice(name='Streaming · configured hourly rate + commission',value='streaming'),app_commands.Choice(name='Packing · employee rate',value='packing')])
async def cmd_clockin(interaction: discord.Interaction, work: app_commands.Choice[str]):
    await clock_in(interaction,work.value)


@bot.tree.command(name='clockout', description='Clock out of a shift')
@app_commands.guild_only()
async def cmd_clockout(interaction: discord.Interaction):
    await clock_out(interaction)


@bot.tree.command(name='startbreak',description='Pause your paid shift for an unpaid break')
@app_commands.guild_only()
async def cmd_startbreak(interaction: discord.Interaction):
    await start_break(interaction)


@bot.tree.command(name='endbreak',description='Return from break and resume your previous shift')
@app_commands.guild_only()
async def cmd_endbreak(interaction: discord.Interaction):
    await end_break(interaction)


# V29: historical Streaming-shift lookup. The shift must belong to the submitting
# employee; never let a user attach commissions to another employee's shift.
def commission_streaming_shift(guild_id, user_id, shift_id=None):
    if shift_id is not None:
        return DB.execute("""SELECT * FROM shifts
             WHERE guild_id=? AND user_id=? AND id=? AND work_type='streaming'""",
             (guild_id, user_id, shift_id)).fetchone()
    return DB.execute("""SELECT * FROM shifts
         WHERE guild_id=? AND user_id=? AND work_type='streaming'
         ORDER BY (clock_out IS NULL) DESC, clock_in DESC LIMIT 1""",
         (guild_id, user_id)).fetchone()


async def commission_shift_autocomplete(interaction: discord.Interaction, current: str):
    if not interaction.guild_id or not interaction.user:
        return []
    rows = DB.execute("""SELECT id,clock_in,clock_out FROM shifts
        WHERE guild_id=? AND user_id=? AND work_type='streaming'
        ORDER BY clock_in DESC LIMIT 100""",
        (interaction.guild_id,interaction.user.id)).fetchall()
    choices = []
    for shift in rows:
        try:
            started = datetime.fromisoformat(shift['clock_in']).astimezone(ZoneInfo('America/New_York'))
            ended = (datetime.fromisoformat(shift['clock_out']).astimezone(ZoneInfo('America/New_York'))
                     if shift['clock_out'] else None)
            label = f"#{shift['id']} | {started:%b %d %I:%M %p} → {ended.strftime('%b %d %I:%M %p') if ended else 'ACTIVE'} ET"
            if not current or current.lower() in label.lower() or current in str(shift['id']):
                choices.append(app_commands.Choice(name=label[:100],value=shift['id']))
        except (TypeError, ValueError):
            continue
        if len(choices) == 25:
            break
    return choices


# The employee-facing submission commands intentionally expose only the required
# fields. Keep the shared implementation private to avoid presenting optional
# payroll/database fields in Discord's slash-command UI.
async def commission(interaction: discord.Interaction, stream_type: app_commands.Choice[str],
                     revenue: str, screenshot: discord.Attachment,
                     pack_size: app_commands.Choice[int] | None = None,
                     breaks: app_commands.Choice[int] | None = None,
                     screenshot2: discord.Attachment | None = None,
                     screenshot3: discord.Attachment | None = None,
                     screenshot4: discord.Attachment | None = None,
                     screenshot5: discord.Attachment | None = None,
                     finished_at_et: str | None = None,
                     shift_id: int | None = None):
    if not allowed_guild(interaction) or not employee(interaction):
        await interaction.response.send_message('Employee access required.',ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        amount=parse_nonnegative(revenue,'100000000')
        cents=int((amount*100).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
        sp_cents=None; character_hours=None; topup=0
        completion = parse_commission_finish(finished_at_et) if finished_at_et else datetime.now(timezone.utc)
        selected_pack=None; break_count=None
        if stream_type.value=='character_break':
            if pack_size is None or breaks is None:
                raise ValueError('For Character Breaks, select pack size (20/50/100) and how many breaks the screenshot includes.')
            selected_pack=pack_size.value
            break_count=breaks.value
            character_hours, earned_cents, topup=character_pack_pay(cents,selected_pack,break_count)
        else:
            if pack_size is not None or breaks is not None:
                raise ValueError('Pack size and break count only apply to Character Breaks.')
            earned_cents=int((Decimal(cents)*STREAM_COMMISSION_RATE).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
    except (InvalidOperation,ValueError) as exc:
        await interaction.followup.send(f'Invalid submission: {exc}',ephemeral=True)
        return
    screenshots = [screenshot] + [a for a in (screenshot2, screenshot3, screenshot4, screenshot5) if a is not None]
    try:
        suffixes = valid_commission_screenshots(screenshots)
    except ValueError as exc:
        await interaction.followup.send(str(exc), ephemeral=True)
        return
    cfg=settings_for(interaction.guild_id)
    review=interaction.guild.get_channel(cfg['review_channel_id']) if cfg['review_channel_id'] else None
    if not isinstance(review,discord.TextChannel):
        await interaction.followup.send('Manager: configure the commission review channel first.',ephemeral=True)
        return
    async with lock:
        shift=commission_streaming_shift(interaction.guild_id,interaction.user.id,shift_id)
        if not shift:
            await interaction.followup.send(
                'No matching Streaming shift was found for your account. Choose a shift from the shift_id suggestions. '
                'If the work was recorded as Packing, ask a manager to correct its work type first.',
                ephemeral=True)
            return
        if character_hours is not None:
            # Guarantee uses the example configured minimum. Base is already paid by the actual shift.
            # Preserve the original shift rate even if the employee's rate later changes.
            character_hours, earned_cents, topup = character_pack_pay(
                cents, selected_pack, break_count,
                int(shift['hourly_rate_cents'] or STREAM_RATE_CENTS))
        shift_begin = datetime.fromisoformat(shift['clock_in'])
        shift_finish = datetime.fromisoformat(shift['clock_out']) if shift['clock_out'] else datetime.now(timezone.utc)
        if finished_at_et is None and shift['clock_out']:
            completion = shift_finish  # after-shift submission: end of the shift unless overridden
        if not (shift_begin <= completion <= shift_finish + timedelta(minutes=1)):
            await interaction.followup.send('Break/set completion must fall within the selected Streaming shift (Eastern Time). Select the correct shift_id and provide finished_at_et for older submissions.',ephemeral=True)
            return
        # The selected shift and completion timestamp must be valid; the fixed
        # Character Break *credited* duration need not fit inside actual worked minutes.
        # Managers still verify completed pack counts, sales and duplicate screenshots.
        cursor=DB.execute("""INSERT INTO commissions
              (shift_id,guild_id,user_id,submitted_at,profit_cents,revenue_cents,order_count,
               rate,commission_cents,attachment_path,stream_type,sales_per_hour_cents,
               character_hours,guarantee_topup_cents,pack_size,break_count,finished_at)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (shift['id'],interaction.guild_id,interaction.user.id,now_iso(),0,cents,None,
               str(STREAM_COMMISSION_RATE),earned_cents,'pending-upload',stream_type.value,
               sp_cents,str(character_hours) if character_hours is not None else None,topup,selected_pack,break_count,completion.isoformat()))
        submission_id=cursor.lastrowid
        DB.commit()
    paths = [UPLOAD_DIR / f'{submission_id}_{i}{suffix}'
             for i, suffix in enumerate(suffixes, 1)]
    try:
        for attachment, path in zip(screenshots, paths):
            await attachment.save(path)
        embed=discord.Embed(title=f'Commission #{submission_id} — Pending',color=discord.Color.orange())
        embed.add_field(name='Employee',value=discord.utils.escape_markdown(interaction.user.display_name))
        embed.add_field(name='Streaming shift',value=f'#{shift["id"]}')
        embed.add_field(name='Format',value='Character Break' if stream_type.value=='character_break' else 'Surprise Set')
        embed.add_field(name='Revenue (all screenshots combined)',value=money(cents))
        embed.add_field(name='Screenshots',value=str(len(paths)))
        embed.add_field(name='Completed (ET)',value=completion.astimezone(ZoneInfo('America/New_York')).strftime('%a %b %d, %Y %I:%M %p'))
        embed.add_field(name='Commission',value=money(earned_cents))
        if character_hours is not None:
            embed.add_field(name='Pack type',value=f'{selected_pack} pack')
            embed.add_field(name='Number of breaks',value=str(break_count))
            embed.add_field(name='Fixed total duration',value=f'{int(CHARACTER_PACK_MINUTES[selected_pack]*break_count)} minutes ({character_hours:.2f} hours)')
            embed.add_field(name='Character guarantee top-up (pending)',value=money(topup))
            embed.add_field(name='Verify before approval',value='Verify the break was completed during the selected Streaming shift, the pack size/count and combined screenshot revenue. Prevent duplicate submissions. Fixed pack duration is a pay credit, not a minimum clocked shift length. Actual hours are paid from the time clock; no extra base hours are added.',inline=False)
        else:
            embed.add_field(name='Verify before approval',value='Verify combined revenue across the screenshots and make sure the same sales are not counted twice.',inline=False)
        embed.set_image(url=f'attachment://proof1{suffixes[0]}')
        # Discord sends all proofs in a single review message; only the first
        # displays as the embed preview, with the others attached below it.
        with ExitStack() as stack:
            files = [discord.File(stack.enter_context(open(path, 'rb')),
                                  filename=f'proof{i}{suffixes[i-1]}')
                     for i, path in enumerate(paths, 1)]
            msg=await review.send(embed=embed, files=files, view=CommissionReview(submission_id))
        async with lock:
            DB.executemany('INSERT INTO commission_attachments (commission_id,ordinal,attachment_path) VALUES (?,?,?)',
                           [(submission_id, i, str(path)) for i, path in enumerate(paths,1)])
            DB.execute('UPDATE commissions SET attachment_path=?,review_message_id=? WHERE id=?',
                       (str(paths[0]),msg.id,submission_id))
            DB.commit()
    except (discord.HTTPException,OSError,sqlite3.Error) as exc:
        async with lock:
            DB.execute('DELETE FROM commission_attachments WHERE commission_id=?',(submission_id,))
            DB.execute('DELETE FROM commissions WHERE id=?',(submission_id,))
            DB.commit()
        for path in paths:
            path.unlink(missing_ok=True)
        logging.exception('Commission submission failed: %s',exc)
        await interaction.followup.send('Upload failed; please try again. If the review post appeared, ask a manager to ignore it.',ephemeral=True)
        return
    await interaction.followup.send(f'✅ {stream_type.name} submission #{submission_id} sent for approval. Shift #{shift["id"]} stays open until you clock out normally.',ephemeral=True)


@bot.tree.command(name='commission', description='Submit Surprise Set or Character Break revenue')
@app_commands.guild_only()
@app_commands.describe(
    stream_type='Choose Surprise Set or Character Break',
    revenue='Total revenue shown across these breaks/sets in the screenshot ($)',
    screenshot='First Whatnot screenshot (required)',
    screenshot2='Second screenshot (optional)',
    screenshot3='Third screenshot (optional)',
    screenshot4='Fourth screenshot (optional)',
    screenshot5='Fifth screenshot (optional)',
    pack_size='Character Break only: choose 20, 50 or 100 pack',
    breaks='Character Break only: number of breaks shown (same pack size, 1–20)',
    finished_at_et='Optional: actual break/set finish as YYYY-MM-DD HH:MM Eastern; use for overnight shifts',
    shift_id='Optional: select your Streaming shift, including historical shifts'
)
@app_commands.choices(stream_type=[
    app_commands.Choice(name='Surprise Set', value='surprise_set'),
    app_commands.Choice(name='Character Break', value='character_break')
], pack_size=[
    app_commands.Choice(name='20 Pack — 40 min', value=20),
    app_commands.Choice(name='50 Pack — 1 hr 30 min', value=50),
    app_commands.Choice(name='100 Pack — 2 hr', value=100)
], breaks=[app_commands.Choice(name=str(i),value=i) for i in range(1,21)])
@app_commands.autocomplete(shift_id=commission_shift_autocomplete)
async def commission_command(interaction: discord.Interaction, stream_type: app_commands.Choice[str],
                             revenue: str, screenshot: discord.Attachment,
                             pack_size: app_commands.Choice[int] | None = None,
                             breaks: app_commands.Choice[int] | None = None,
                             screenshot2: discord.Attachment | None = None,
                             screenshot3: discord.Attachment | None = None,
                             screenshot4: discord.Attachment | None = None,
                             screenshot5: discord.Attachment | None = None,
                     finished_at_et: str | None = None,
                     shift_id: int | None = None):
    await commission(interaction, stream_type, revenue, screenshot, pack_size, breaks,
                     screenshot2, screenshot3, screenshot4, screenshot5, finished_at_et, shift_id)


@bot.tree.command(name='hours', description='Manager: view employee hours, live shifts and estimated wages')
@app_commands.describe(days='Lookback period: 1–90 days (default 7)', employee='Optional: view just one employee')
@app_commands.guild_only()
async def hours(interaction: discord.Interaction, days: app_commands.Range[int, 1, 90] = 7,
                employee: discord.Member | None = None):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)
    sql = 'SELECT * FROM shifts WHERE guild_id=? AND (clock_out IS NULL OR clock_out>=?)'
    params = [interaction.guild_id, cutoff.isoformat()]
    if employee:
        sql += ' AND user_id=?'
        params.append(employee.id)
    shift_rows = DB.execute(sql, params).fetchall()
    aggregates = {}
    for shift in shift_rows:
        user_id = shift['user_id']
        entry = aggregates.setdefault(user_id, {'seconds': 0, 'wages': 0, 'active': 0, 'completed': 0, 'commissions': 0, 'topups': 0})
        start = datetime.fromisoformat(shift['clock_in'])
        if shift['clock_out'] is None:
            entry['active'] += 1
            continue
        stop = datetime.fromisoformat(shift['clock_out'])
        # Only count time inside the chosen reporting window.
        span = max(0, (stop - max(start, cutoff)).total_seconds())
        entry['seconds'] += span
        rate = shift['hourly_rate_cents'] if shift['hourly_rate_cents'] is not None else HOURLY_RATE_CENTS
        entry['wages'] += int((Decimal(str(span)) / Decimal('3600') * int(rate)).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
        entry['completed'] += 1
        # Commissions here are linked to shifts completed in the selected window.
        approved = DB.execute("SELECT COALESCE(SUM(commission_cents),0) AS amount,COALESCE(SUM(guarantee_topup_cents),0) AS topup FROM commissions WHERE shift_id=? AND status='approved'", (shift['id'],)).fetchone()
        entry['commissions'] += approved['amount']
        entry['topups'] += approved['topup']
    await preload_employee_names(interaction.guild, aggregates.keys())
    embed = discord.Embed(title='📊 ETBeastDemo Employee Hours',
                          description=f'Last {days} days • Completed hours only (active shifts shown separately).',
                          color=discord.Color.blurple())
    if not aggregates:
        embed.description += '\nNo completed or active shifts found.'
    else:
        sorted_rows = sorted(aggregates.items(), key=lambda item: (-item[1]['seconds'], item[0]))
        for user_id, stats in sorted_rows[:20]:
            total = stats['wages'] + stats['commissions'] + stats['topups']
            embed.add_field(name=employee_name(interaction.guild, user_id),
                            value=(f'**{Decimal(str(stats["seconds"])) / Decimal("3600"):.2f} hrs** • '
                                   f'{stats["completed"]} completed shifts'
                                   + (' • 🟢 Clocked in' if stats['active'] else '')
                                   + f'\nBase: {money(stats["wages"])} | Commission: {money(stats["commissions"])} | Character top-up: {money(stats["topups"])}'
                                   + f'\nEstimated total: **{money(total)}**'), inline=False)
        if len(sorted_rows) > 20:
            embed.set_footer(text=f'Showing 20 of {len(sorted_rows)} employees. Use /hours employee: or /payroll for details.')
    embed.add_field(name='Payroll notice', value='Estimates only; no overtime, breaks, tax or deduction calculations.', inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


def performance_records(guild_id, days, employee_id=None):
    """Group approved revenue by employee; only closed shifts with approved revenue are counted.

    Window is based on shift completion (UTC). This is operational reporting, not payroll.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    sql = """SELECT s.user_id,s.id,s.clock_in,s.clock_out,
              c.revenue_cents,c.order_count,c.commission_cents,c.status
             FROM commissions c JOIN shifts s ON s.id=c.shift_id
             WHERE s.guild_id=? AND s.clock_out IS NOT NULL AND s.clock_out>=?
               AND c.guild_id=? AND c.revenue_cents IS NOT NULL AND c.status='approved'"""
    params = [guild_id, cutoff, guild_id]
    if employee_id is not None:
        sql += ' AND s.user_id=?'
        params.append(employee_id)
    result = {}
    seen_shifts = set()
    for r in DB.execute(sql, params):
        stats = result.setdefault(r['user_id'], {'revenue':0,'seconds':0,'orders':0,'order_sessions':0,
                                                'commission':0,'streams':0})
        stats['revenue'] += r['revenue_cents']
        stats['commission'] += r['commission_cents']
        stats['streams'] += 1
        if r['id'] not in seen_shifts:
            stats['seconds'] += max(0,(datetime.fromisoformat(r['clock_out'])-
                                      datetime.fromisoformat(r['clock_in'])).total_seconds())
            seen_shifts.add(r['id'])
        if r['order_count'] is not None:
            stats['orders'] += r['order_count']
            stats['order_sessions'] += 1
    return result


def performance_embed(guild, days, selected_id=None):
    records = performance_records(guild.id, days, selected_id)
    e = discord.Embed(title='📈 ETBeastDemo Revenue Performance',
                      description=f'Last {days} days • Approved **revenue** submissions only • Ranked by revenue/hour',
                      color=discord.Color.blurple())
    if not records:
        e.add_field(name='No results', value='No approved revenue submissions in this period. Older profit-based submissions are deliberately excluded.', inline=False)
        return e
    ordered = sorted(records.items(), key=lambda v: (-(v[1]['revenue'] / v[1]['seconds'] if v[1]['seconds'] else 0), v[0]))
    for idx,(uid,r) in enumerate(ordered[:15],1):
        hours = Decimal(str(r['seconds'])) / Decimal('3600')
        rev_hr = int((Decimal(r['revenue'])/hours).quantize(Decimal('1'), rounding=ROUND_HALF_UP)) if hours else 0
        name = employee_name(guild, uid)
        details = (f'**Revenue:** {money(r["revenue"])} • **Stream hours:** {hours:.2f} • '
                   f'**Revenue/hour:** {money(rev_hr)}\n'
                   f'**Approved segments:** {r["streams"]} • **Commission:** {money(r["commission"])}')
        if r['order_sessions'] == r['streams'] and r['orders']:
            aov = int(Decimal(r['revenue']) / r['orders'])
            details += f'\n**Orders:** {r["orders"]} • **AOV:** {money(aov)}'
        elif r['orders']:
            details += f'\nOrders/AOV omitted (not provided for every submission)'
        e.add_field(name=f'{idx}. {name}', value=details, inline=False)
    if len(ordered)>15:
        e.set_footer(text=f'Showing 15 of {len(ordered)} employees. Use /performance employee: for a specific person.')
    e.add_field(name='Data scope',value='Revenue and hours are linked only to approved, revenue-based submissions. Refunds, canceled orders and timing adjustments require management review. Legacy profit records are not counted.',inline=False)
    return e


@bot.tree.command(name='performance', description='Manager: revenue leaderboard and streamer performance')
@app_commands.describe(days='Lookback: 1–90 days', employee='Optional individual employee')
@app_commands.guild_only()
async def performance(interaction: discord.Interaction, days: app_commands.Range[int,1,90]=7,
                      employee: discord.Member | None = None):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    await preload_employee_names(interaction.guild, performance_records(interaction.guild_id, days, employee.id if employee else None).keys())
    await interaction.followup.send(embed=performance_embed(interaction.guild, days, employee.id if employee else None), ephemeral=True)


@bot.tree.command(name='postperformance', description='Manager: publish a revenue report to the private performance channel')
@app_commands.describe(days='Lookback: 1–90 days')
@app_commands.guild_only()
async def postperformance(interaction: discord.Interaction, days: app_commands.Range[int,1,90]=7):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    cid = settings_for(interaction.guild_id)['performance_channel_id']
    channel = interaction.guild.get_channel(cid) if cid else None
    if not isinstance(channel, discord.TextChannel):
        await interaction.response.send_message('Set a private Performance channel in /settings first.',ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    await preload_employee_names(interaction.guild, performance_records(interaction.guild_id, days).keys())
    try:
        await channel.send(embed=performance_embed(interaction.guild, days),
                           allowed_mentions=discord.AllowedMentions.none())
    except discord.HTTPException:
        await interaction.followup.send('Could not post report: check bot channel permissions.',ephemeral=True)
        return
    await interaction.followup.send(f'✅ Posted {days}-day revenue report in {channel.mention}.',ephemeral=True)


@bot.tree.command(name='performancecsv', description='Manager: export approved revenue performance as CSV')
@app_commands.describe(days='Lookback: 1–90 days')
@app_commands.guild_only()
async def performancecsv(interaction: discord.Interaction, days: app_commands.Range[int,1,90]=7):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    records = performance_records(interaction.guild_id, days)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['discord_user_id','approved_streams','stream_hours','approved_revenue_usd',
                     'revenue_per_hour_usd','approved_commission_usd','orders_if_complete','aov_usd_if_complete'])
    for uid,stat in sorted(records.items(),key=lambda item:-item[1]['revenue']):
        hours = Decimal(str(stat['seconds'])) / 3600
        per_hour = Decimal(stat['revenue']) / 100 / hours if hours else Decimal(0)
        all_orders = stat['order_sessions']==stat['streams'] and stat['orders']>0
        aov = Decimal(stat['revenue']) / 100 / stat['orders'] if all_orders else None
        writer.writerow([uid,stat['streams'],f'{hours:.4f}',money(stat['revenue']),f'{per_hour:.2f}',
                         money(stat['commission']),stat['orders'] if all_orders else '',f'{aov:.2f}' if aov is not None else ''])
    await interaction.response.send_message(content=f'Approved revenue performance, last {days} days. Legacy profit records excluded.',
                                            file=discord.File(io.BytesIO(output.getvalue().encode('utf-8-sig')),
                                                              filename='etbeast_demo_revenue_performance.csv'),ephemeral=True)


@bot.tree.command(name='bonus', description='Manager: award a one-time employee bonus')
@app_commands.describe(employee='Employee receiving the bonus', amount='Bonus in USD, e.g. 18.00', reason='Why the bonus was awarded')
@app_commands.guild_only()
async def bonus(interaction: discord.Interaction, employee: discord.Member, amount: str, reason: str):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    try:
        dollars = parse_nonnegative(amount, '100000')
        cents = int((dollars * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
        if cents <= 0:
            raise ValueError
    except ValueError:
        await interaction.response.send_message('Enter a positive USD amount up to $100,000 (example: 25.00).', ephemeral=True)
        return
    reason = reason.strip()
    if not reason or len(reason) > 250:
        await interaction.response.send_message('Provide a reason between 1 and 250 characters.', ephemeral=True)
        return
    async with lock:
        cursor = DB.execute(
            'INSERT INTO bonuses(guild_id,employee_id,amount_cents,reason,created_at,created_by) VALUES (?,?,?,?,?,?)',
            (interaction.guild_id, employee.id, cents, reason, now_iso(), interaction.user.id))
        bonus_id = cursor.lastrowid
        DB.commit()
    await interaction.response.send_message(
        f'✅ Bonus #{bonus_id}: {money(cents)} awarded to {employee.mention}. Reason: {discord.utils.escape_mentions(reason)}. '
        'Included in payroll exports according to the date it was awarded.', ephemeral=True)
    try:
        await employee.send(f'🎁 ETBeastDemo bonus awarded: {money(cents)}. Reason: {reason}. Bonus ID: {bonus_id}.')
    except (discord.Forbidden, discord.HTTPException):
        pass


@bot.tree.command(name='bonuses', description='Manager: view recently awarded and voided bonuses')
@app_commands.describe(days='Lookback days, default 30', employee='Filter to one employee')
@app_commands.guild_only()
async def bonuses(interaction: discord.Interaction, days: app_commands.Range[int, 1, 90] = 30,
                  employee: discord.Member | None = None):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    query = 'SELECT * FROM bonuses WHERE guild_id=? AND created_at>=?'
    params = [interaction.guild_id, cutoff]
    if employee:
        query += ' AND employee_id=?'
        params.append(employee.id)
    query += ' ORDER BY id DESC LIMIT 25'
    rows = DB.execute(query, params).fetchall()
    if not rows:
        await interaction.response.send_message('No bonuses in that period.', ephemeral=True)
        return
    await preload_employee_names(interaction.guild, [r['employee_id'] for r in rows])
    active_deductions={r['incident_id']:r['week_start'] for r in DB.execute(
        'SELECT incident_id,week_start FROM fine_payroll_deductions WHERE guild_id=?',
        (interaction.guild_id,)).fetchall()}
    lines = [f'#{r["id"]} • {employee_name(interaction.guild,r["employee_id"])} • {money(r["amount_cents"])} • '
             f'{"VOID" if r["voided_at"] else "ACTIVE"} • {discord.utils.escape_mentions(r["reason"][:75])}' for r in rows]
    await interaction.response.send_message('**Recent bonuses (up to 25):**\n' + '\n'.join(lines), ephemeral=True)


@bot.tree.command(name='voidbonus', description='Manager: void a bonus with an audit trail')
@app_commands.describe(bonus_id='Bonus ID from /bonuses', reason='Reason for voiding the bonus')
@app_commands.guild_only()
async def voidbonus(interaction: discord.Interaction, bonus_id: int, reason: str):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    reason = reason.strip()
    if not reason or len(reason) > 250:
        await interaction.response.send_message('Provide a reason between 1 and 250 characters.', ephemeral=True)
        return
    async with lock:
        r = DB.execute('SELECT * FROM bonuses WHERE guild_id=? AND id=?',
                       (interaction.guild_id, bonus_id)).fetchone()
        if not r or r['voided_at']:
            await interaction.response.send_message('Bonus not found or already voided.', ephemeral=True)
            return
        DB.execute('UPDATE bonuses SET voided_at=?,voided_by=?,void_reason=? WHERE id=?',
                   (now_iso(), interaction.user.id, reason, bonus_id))
        DB.commit()
    await interaction.response.send_message(
        f'✅ Bonus #{bonus_id} voided. It will no longer appear as payable in new payroll exports. '
        'If already paid, handle the adjustment through your payroll process.', ephemeral=True)


# V38: manager-only historical shift voids and force clock-out.
# Voids are archived before deletion because legacy payroll reads directly from shifts.
DB.executescript("""
CREATE TABLE IF NOT EXISTS voided_shift_archive (
 id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
 shift_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
 original_json TEXT NOT NULL, voided_by INTEGER NOT NULL,
 voided_at TEXT NOT NULL, reason TEXT NOT NULL,
 UNIQUE(guild_id,shift_id)
);
""")
DB.commit()


def guarded_shift_weeks(guild_id, shift):
    """Refuse any already-approved/paid or externally paid periods touched by a shift."""
    start = datetime.fromisoformat(shift['clock_in'])
    end = datetime.fromisoformat(shift['clock_out']) if shift['clock_out'] else datetime.now(timezone.utc)
    if end < start:
        raise ValueError('Shift has invalid time ordering; review it manually.')
    cursor = payroll_week_start(start)
    final_week = payroll_week_start(end)
    while cursor <= final_week:
        week = cursor.isoformat()
        if payroll_week_status(guild_id, week):
            raise ValueError(f'Payroll week {week} is finalized; this shift needs a formal payroll adjustment.')
        payment = DB.execute("""SELECT 1 FROM employee_payments WHERE guild_id=? AND employee_id=?
            AND week_start=? AND voided_at IS NULL LIMIT 1""",
            (guild_id,shift['user_id'],week)).fetchone()
        if payment:
            raise ValueError(f'Payroll week {week} has recorded payments; resolve them before changing this shift.')
        cursor += timedelta(days=7)


def void_clocked_shift_record(guild_id, shift_id, actor_id, reason):
    reason = reason.strip()
    if not 5 <= len(reason) <= 300:
        raise ValueError('Reason must contain 5–300 characters.')
    with DB:
        shift = DB.execute('SELECT * FROM shifts WHERE guild_id=? AND id=?',
                           (guild_id,shift_id)).fetchone()
        if shift is None:
            raise ValueError('Shift not found (it may already have been voided).')
        if shift['clock_out'] is None:
            raise ValueError('Active shift: use Force Clock Out first, then review before voiding.')
        linked = DB.execute('SELECT id,status FROM commissions WHERE shift_id=? LIMIT 1',
                            (shift_id,)).fetchone()
        if linked:
            raise ValueError(f'Shift has commission #{linked["id"]} ({linked["status"]}). '
                             'Resolve all linked submissions before voiding this shift.')
        linked_break = DB.execute("""SELECT id FROM employee_breaks WHERE guild_id=?
              AND (prior_shift_id=? OR next_shift_id=?) LIMIT 1""",
              (guild_id,shift_id,shift_id)).fetchone()
        if linked_break:
            raise ValueError('Shift is linked to a meal break. Manager must review the break history before voiding.')
        guarded_shift_weeks(guild_id, shift)
        DB.execute("""INSERT INTO voided_shift_archive
             (guild_id,shift_id,user_id,original_json,voided_by,voided_at,reason)
             VALUES(?,?,?,?,?,?,?)""",
             (guild_id,shift_id,shift['user_id'],json.dumps(dict(shift),default=str),
              actor_id,now_iso(),reason))
        shift_audit(guild_id,shift_id,shift['user_id'],'void',shift['clock_in'],
                    shift['clock_out'],None,None,reason,actor_id)
        DB.execute('DELETE FROM shifts WHERE guild_id=? AND id=?',(guild_id,shift_id))
        return dict(shift)


def force_clock_out_record(guild_id, user_id, actor_id, reason, end_time=None):
    reason=reason.strip()
    if not 5 <= len(reason) <= 300:
        raise ValueError('Reason must contain 5–300 characters.')
    with DB:
        shift=DB.execute('SELECT * FROM shifts WHERE guild_id=? AND user_id=? AND clock_out IS NULL',
                         (guild_id,user_id)).fetchone()
        if not shift:
            if open_break(guild_id,user_id):
                raise ValueError('Employee is on an unpaid meal break, not clocked in. Review the break before making changes.')
            raise ValueError('Employee has no active shift.')
        start=datetime.fromisoformat(shift['clock_in'])
        finish=parse_shift_time(end_time) if end_time else datetime.now(timezone.utc)
        # Validate the target time without crossing pre-existing shifts.
        validate_interval(guild_id,user_id,start,finish,shift['id'])
        guard=dict(shift); guard['clock_out']=finish.isoformat()
        guarded_shift_weeks(guild_id,guard)
        DB.execute('UPDATE shifts SET clock_out=? WHERE guild_id=? AND id=? AND clock_out IS NULL',
                   (finish.isoformat(),guild_id,shift['id']))
        shift_audit(guild_id,shift['id'],user_id,'force_clock_out',shift['clock_in'],
                    None,shift['clock_in'],finish.isoformat(),reason,actor_id)
        return dict(shift),finish.isoformat()


@bot.tree.command(name='voidshift',description='Manager: void an erroneous closed payroll shift')
@app_commands.describe(shift_id='Shift ID from /shiftlog',reason='Reason for removing this payroll shift')
@app_commands.guild_only()
async def voidshift(interaction:discord.Interaction,shift_id:int,reason:str):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.',ephemeral=True)
    try:
        async with lock:
            removed=void_clocked_shift_record(interaction.guild_id,shift_id,interaction.user.id,reason)
    except (ValueError,sqlite3.IntegrityError) as exc:
        return await interaction.response.send_message(f'❌ {exc}',ephemeral=True)
    await interaction.response.send_message(f'✅ Shift #{shift_id} voided for <@{removed["user_id"]}>. '
        'Excluded from future payroll calculations; archived with reason and manager ID. '
        'Review the employee’s weekly payroll before finalizing.',ephemeral=True)
    await notify_shift_correction(interaction,f'🗑️ **Shift #{shift_id} voided** by {interaction.user.display_name}. '
        f'Employee ID: {removed["user_id"]}. Reason: {reason}')


@bot.tree.command(name='forceclockout',description='Manager: close an employee’s active paid shift')
@app_commands.describe(employee='Employee to clock out',reason='Required explanation',
    clock_out='Optional exact ISO time with offset, e.g. 2030-01-12T17:43:00-04:00; blank = now')
@app_commands.guild_only()
async def forceclockout(interaction:discord.Interaction,employee:discord.Member,reason:str,clock_out:str=''):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.',ephemeral=True)
    try:
        async with lock:
            shift,finish=force_clock_out_record(interaction.guild_id,employee.id,interaction.user.id,
                                               reason,clock_out.strip() or None)
    except ValueError as exc:
        return await interaction.response.send_message(f'❌ {exc}',ephemeral=True)
    await interaction.response.send_message(f'✅ Force-clocked out {employee.mention} from '
        f'{shift["work_type"].title()} shift #{shift["id"]} at {finish}. '
        'Review their payroll and commissions.',ephemeral=True)
    event_id=enqueue_clock_log(interaction.guild_id,employee.id,employee.display_name,'out',
        shift['id'],shift['clock_in'],finish,shift['hourly_rate_cents'],event_type='manager_force_out')
    if event_id is not None:
        await deliver_employee_log(event_id)
    await notify_shift_correction(interaction,f'🔒 **Manager force clock-out**: {employee.display_name} '
        f'#{shift["id"]} at {finish}; manager: {interaction.user.display_name}; reason: {reason}')


# -------- V6 manager shift corrections --------
def parse_shift_time(value: str):
    # Require an explicit UTC offset to prevent daylight-saving/timezone ambiguity.
    try:
        dt = datetime.fromisoformat(value.strip().replace('Z','+00:00'))
    except ValueError:
        raise ValueError('Use ISO time with timezone, e.g. 2030-01-12T15:00:00-04:00')
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError('Timezone offset required, e.g. -04:00 or -05:00.')
    return dt.astimezone(timezone.utc)


def validate_interval(guild_id, user_id, start, end, exclude_shift_id=None):
    if start > datetime.now(timezone.utc) + timedelta(minutes=5):
        raise ValueError('Clock-in cannot be in the future.')
    if end and (end <= start or end > datetime.now(timezone.utc) + timedelta(minutes=5)):
        raise ValueError('Clock-out must be later than clock-in and not in the future.')
    if end and end - start > timedelta(hours=24):
        raise ValueError('Shifts over 24 hours require separate manual payroll review; do not fabricate a clock-out.')
    records = DB.execute('SELECT id,clock_in,clock_out FROM shifts WHERE guild_id=? AND user_id=?',
                         (guild_id,user_id)).fetchall()
    for row in records:
        if row['id'] == exclude_shift_id:
            continue
        other_start = datetime.fromisoformat(row['clock_in'])
        other_end = datetime.fromisoformat(row['clock_out']) if row['clock_out'] else None
        # Intervals touch without overlapping. Open end means infinity.
        if (end is None or other_start < end) and (other_end is None or start < other_end):
            raise ValueError(f'Overlaps existing shift #{row["id"]}. Resolve that shift first.')


def shift_audit(guild_id, row_id, user_id, action, old_start, old_end, new_start, new_end, reason, actor_id):
    DB.execute("""INSERT INTO shift_audit
      (guild_id,shift_id,user_id,action,old_start,old_end,new_start,new_end,reason,actor_id,created_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
      (guild_id,row_id,user_id,action,old_start,old_end,new_start,new_end,reason,actor_id,now_iso()))


async def notify_shift_correction(interaction, text):
    cfg = settings_for(interaction.guild_id)
    channel = interaction.guild.get_channel(cfg['log_channel_id']) if cfg['log_channel_id'] else None
    if isinstance(channel,discord.TextChannel):
        try:
            await channel.send(text,allowed_mentions=discord.AllowedMentions.none())
        except (discord.HTTPException,discord.Forbidden):
            logging.exception('Could not post manager correction log')


def shift_rate_for_type(work_type):
    if work_type == 'streaming':
        return STREAM_RATE_CENTS
    if work_type == 'packing':
        return PACK_RATE_CENTS
    raise ValueError('Choose Streaming or Packing for the shift.')


def shift_correction_rate(guild_id, user_id, previous_type, previous_rate_cents, selected_type):
    """Preserve historical wages for time-only edits; use custom rates on type changes."""
    if previous_type == selected_type:
        return int(previous_rate_cents)
    return shift_rate_for_employee(guild_id, user_id, selected_type)


def audit_shift_work_type(guild_id, shift_id, old_type, new_type, old_rate, new_rate):
    DB.execute('''UPDATE shift_audit SET old_work_type=?,new_work_type=?,old_rate_cents=?,new_rate_cents=?
                  WHERE id=(SELECT MAX(id) FROM shift_audit WHERE guild_id=? AND shift_id=?)''',
               (old_type,new_type,old_rate,new_rate,guild_id,shift_id))


def validate_shift_type_correction(shift_id, old_type, new_type):
    if new_type != old_type and new_type == 'packing':
        linked=DB.execute("SELECT COUNT(*) AS n FROM commissions WHERE shift_id=? AND status IN ('pending','approved')",(shift_id,)).fetchone()['n']
        if linked:
            raise ValueError('This shift has pending/approved streaming commissions. Resolve them before switching to Packing.')

@bot.tree.command(name='setpackrate', description='Manager: set an employee’s Packing hourly rate for future shifts')
@app_commands.describe(employee='Employee receiving the rate', hourly_rate='New Packing hourly rate in dollars, e.g. 18.00', reason='Why this rate changed')
@app_commands.guild_only()
async def setpackrate(interaction:discord.Interaction, employee:discord.Member, hourly_rate:str, reason:str):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.',ephemeral=True)
    if len(reason.strip()) < 5:
        return await interaction.response.send_message('Provide a reason of at least five characters.',ephemeral=True)
    try:
        rate=Decimal(hourly_rate)
        if not rate.is_finite() or rate<=0 or rate>1000:
            raise ValueError('Rate must be greater than $0 and at most $1,000/hour.')
        cents=int((rate*100).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
    except (InvalidOperation,ValueError) as exc:
        return await interaction.response.send_message(f'Invalid rate: {exc}',ephemeral=True)
    async with lock:
        old=packing_rate_for_employee(interaction.guild_id,employee.id)
        DB.execute('''INSERT INTO employee_pack_rates (guild_id,user_id,rate_cents,set_at,set_by)
                      VALUES (?,?,?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET
                      rate_cents=excluded.rate_cents,set_at=excluded.set_at,set_by=excluded.set_by''',
                   (interaction.guild_id,employee.id,cents,now_iso(),interaction.user.id))
        DB.execute('''INSERT INTO settings_audit(guild_id,actor_id,changed_at,setting,old_value,new_value)
                      VALUES (?,?,?,?,?,?)''',(interaction.guild_id,interaction.user.id,now_iso(),
                      f'packing_rate:{employee.id}',str(old),str(cents)))
        DB.commit()
    await interaction.response.send_message(
        f'✅ {employee.mention} Packing rate: {money(old)} → **{money(cents)}/hr**. '
        'Existing shifts and active shift segments are unchanged. The new rate applies on the next Packing clock-in or switch. '
        f'Reason: {reason}',ephemeral=True)


@bot.tree.command(name='setallpackrates', description='Manager: update Packing hourly rates for all current staff')
@app_commands.describe(hourly_rate='New Packing hourly rate in dollars, e.g. 18.00', reason='Why all Packing rates are changing', confirm='Choose True to confirm changing everyone’s Packing rate')
@app_commands.guild_only()
async def setallpackrates(interaction:discord.Interaction, hourly_rate:str, reason:str, confirm:bool):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.', ephemeral=True)
    if not confirm:
        return await interaction.response.send_message('No rates changed. Choose confirm=True to apply this rate to all current staff.',ephemeral=True)
    if len(reason.strip()) < 5:
        return await interaction.response.send_message('Provide a reason of at least five characters.',ephemeral=True)
    try:
        rate=Decimal(hourly_rate)
        if not rate.is_finite() or rate<=0 or rate>1000:
            raise ValueError('Rate must be greater than $0 and at most $1,000/hour.')
        cents=int((rate*100).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
    except (InvalidOperation,ValueError) as exc:
        return await interaction.response.send_message(f'Invalid rate: {exc}',ephemeral=True)
    # A known Employee role is essential: otherwise 'everyone' could include customers
    # or other unrelated server members. Managers are included as current staff.
    if not EMPLOYEE_ROLE_ID:
        return await interaction.response.send_message(
            'Bulk rate changes require EMPLOYEE_ROLE_ID configured in .env so only staff are affected.',ephemeral=True)
    guild=interaction.guild
    if guild is None or guild.get_role(EMPLOYEE_ROLE_ID) is None:
        return await interaction.response.send_message('Employee role could not be found in this server.',ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        members=[m async for m in guild.fetch_members(limit=None)]
    except (discord.Forbidden,discord.HTTPException) as exc:
        return await interaction.followup.send(f'Could not retrieve staff list: {exc}',ephemeral=True)
    staff=[m for m in members if not m.bot and (
        any(r.id==EMPLOYEE_ROLE_ID for r in m.roles) or
        (MANAGER_ROLE_ID and any(r.id==MANAGER_ROLE_ID for r in m.roles)) or
        m.guild_permissions.manage_guild
    )]
    if not staff:
        return await interaction.followup.send('No staff found. No rates changed.',ephemeral=True)
    timestamp=now_iso()
    async with lock:
        with DB:
            for member in staff:
                old=packing_rate_for_employee(interaction.guild_id,member.id)
                DB.execute('''INSERT INTO employee_pack_rates (guild_id,user_id,rate_cents,set_at,set_by)
                              VALUES (?,?,?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET
                              rate_cents=excluded.rate_cents,set_at=excluded.set_at,set_by=excluded.set_by''',
                           (interaction.guild_id,member.id,cents,timestamp,interaction.user.id))
                DB.execute('''INSERT INTO settings_audit(guild_id,actor_id,changed_at,setting,old_value,new_value)
                              VALUES (?,?,?,?,?,?)''',
                           (interaction.guild_id,interaction.user.id,timestamp,
                            f'packing_rate:{member.id}',str(old),str(cents)))
    await interaction.followup.send(
        f'✅ Updated Packing rates for **{len(staff)} current staff members** to **{money(cents)}/hr**. '
        'Existing shifts and active shift segments were not changed; the new rate applies on the next Packing clock-in or switch. '
        f'Reason: {reason}',ephemeral=True)


# V44: Correct historical Packing wages without editing clock times or future rates.
def parse_manager_hourly_rate(hourly_rate):
    try:
        rate = Decimal(hourly_rate)
        if not rate.is_finite() or rate <= 0 or rate > 1000:
            raise ValueError('Rate must be greater than $0 and at most $1,000/hour.')
        return int((rate * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f'Invalid hourly rate: {exc}') from exc


def eligible_packing_rate_change(guild_id, shift):
    if shift['work_type'] != 'packing':
        raise ValueError(f'Shift #{shift["id"]} is not a Packing shift.')
    if shift['clock_out'] is None:
        raise ValueError(f'Shift #{shift["id"]} is still active. Its rate cannot be retroactively changed.')
    guarded_shift_weeks(guild_id, shift)


def fix_packing_shift_rate(guild_id, shift_id, cents, actor_id, reason):
    with DB:
        shift = DB.execute('SELECT * FROM shifts WHERE guild_id=? AND id=?',
                           (guild_id, shift_id)).fetchone()
        if shift is None:
            raise ValueError('Shift not found.')
        eligible_packing_rate_change(guild_id, shift)
        previous = int(shift['hourly_rate_cents'] if shift['hourly_rate_cents'] is not None else PACK_RATE_CENTS)
        if previous == cents:
            return shift, previous, False
        DB.execute('UPDATE shifts SET hourly_rate_cents=? WHERE guild_id=? AND id=?',
                   (cents, guild_id, shift_id))
        shift_audit(guild_id, shift_id, shift['user_id'], 'edit',
                    shift['clock_in'], shift['clock_out'], shift['clock_in'], shift['clock_out'],
                    reason, actor_id)
        audit_shift_work_type(guild_id, shift_id, 'packing', 'packing', previous, cents)
        return shift, previous, True


@bot.tree.command(name='fixpackingrate', description='Manager: correct the hourly pay rate on one completed Packing shift')
@app_commands.describe(shift_id='Shift number from /shiftlog', hourly_rate='Correct rate in dollars, e.g. 15.00', reason='Reason for the pay correction')
@app_commands.guild_only()
async def fixpackingrate(interaction: discord.Interaction, shift_id: int, hourly_rate: str, reason: str):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.', ephemeral=True)
    if not 5 <= len(reason.strip()) <= 300:
        return await interaction.response.send_message('Reason must contain 5–300 characters.', ephemeral=True)
    try:
        cents = parse_manager_hourly_rate(hourly_rate)
        async with lock:
            shift, old, changed = fix_packing_shift_rate(
                interaction.guild_id, shift_id, cents, interaction.user.id, reason.strip())
    except ValueError as exc:
        return await interaction.response.send_message(f'❌ {exc}', ephemeral=True)
    if not changed:
        return await interaction.response.send_message(f'Shift #{shift_id} is already {money(cents)}/hr. No change made.', ephemeral=True)
    await interaction.response.send_message(
        f'✅ Packing shift #{shift_id} updated: **{money(old)}/hr → {money(cents)}/hr**. '
        'Original clock times, other shifts, active shifts, and future employee rates were not changed. '
        'Unfinalized payroll calculations will reflect the corrected rate.', ephemeral=True)
    await notify_shift_correction(interaction,
        f'📝 Packing rate correction by {interaction.user.display_name}: shift #{shift_id}, '
        f'employee <@{shift["user_id"]}>, {money(old)}/hr → {money(cents)}/hr. Reason: {reason.strip()}')


@bot.tree.command(name='fixpackerhistory', description='Manager: preview or correct one employee’s completed, unpaid Packing shifts')
@app_commands.describe(employee='New packer whose previous shifts need correction',
                       hourly_rate='Correct historical Packing hourly rate, e.g. 15.00',
                       reason='Reason for historical correction',
                       confirm='False previews; True applies to all eligible shifts',
                       since='Optional earliest shift date, YYYY-MM-DD (Eastern time)')
@app_commands.guild_only()
async def fixpackerhistory(interaction: discord.Interaction, employee: discord.Member,
                           hourly_rate: str, reason: str, confirm: bool = False,
                           since: str | None = None):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.', ephemeral=True)
    if not 5 <= len(reason.strip()) <= 300:
        return await interaction.response.send_message('Reason must contain 5–300 characters.', ephemeral=True)
    try:
        cents = parse_manager_hourly_rate(hourly_rate)
        after = date.fromisoformat(since) if since else None
    except ValueError as exc:
        return await interaction.response.send_message(f'❌ {exc}', ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    async with lock:
        shifts = DB.execute("SELECT * FROM shifts WHERE guild_id=? AND user_id=? "
                            "AND work_type='packing' ORDER BY clock_in, id",
                            (interaction.guild_id, employee.id)).fetchall()
        eligible, skipped, unchanged = [], [], 0
        for shift in shifts:
            if after and datetime.fromisoformat(shift['clock_in']).astimezone(PAYROLL_TZ).date() < after:
                continue
            old = int(shift['hourly_rate_cents'] if shift['hourly_rate_cents'] is not None else PACK_RATE_CENTS)
            if old == cents:
                unchanged += 1
                continue
            try:
                eligible_packing_rate_change(interaction.guild_id, shift)
            except ValueError as exc:
                skipped.append(f'#{shift["id"]}: {exc}')
                continue
            eligible.append((shift, old))
        if confirm and eligible:
            # Revalidate inside the transaction; no partial bulk updates.
            with DB:
                for shift, old in eligible:
                    eligible_packing_rate_change(interaction.guild_id, shift)
                    DB.execute('UPDATE shifts SET hourly_rate_cents=? WHERE guild_id=? AND id=?',
                               (cents, interaction.guild_id, shift['id']))
                    shift_audit(interaction.guild_id, shift['id'], employee.id, 'edit',
                                shift['clock_in'], shift['clock_out'], shift['clock_in'], shift['clock_out'],
                                reason.strip(), interaction.user.id)
                    audit_shift_work_type(interaction.guild_id, shift['id'], 'packing', 'packing', old, cents)
    label = '✅ UPDATED' if confirm else '🔎 PREVIEW ONLY'
    ids = ', '.join(f'#{shift["id"]}' for shift, _ in eligible)
    message = (f'{label}: {employee.mention} — target **{money(cents)}/hr**.\n'
               f'Eligible shifts: **{len(eligible)}** ({ids[:700] or "none"})\n'
               f'Already at target: {unchanged}. Skipped/protected: {len(skipped)}.\n'
               + ('\n'.join(skipped[:5]) if skipped else '') +
               ('\n**Nothing has been changed.** Run again with confirm=True to apply.' if not confirm else
                '\nClock times, active shifts, other employees, and future rates unchanged.'))
    await interaction.followup.send(message[:1900], ephemeral=True)
    if confirm and eligible:
        await notify_shift_correction(interaction,
            f'📝 Historical Packing rate correction by {interaction.user.display_name} for '
            f'{employee.display_name}: {len(eligible)} completed shifts → {money(cents)}/hr. '
            f'Shift IDs: {ids[:1000]}. Reason: {reason.strip()}')


@bot.tree.command(name='setstreamrate', description='Manager: set one employee’s Streaming hourly rate for future shifts')
@app_commands.describe(employee='Employee receiving the rate', hourly_rate='New hourly rate in dollars, e.g. 18.00', reason='Why this rate changed')
@app_commands.guild_only()
async def setstreamrate(interaction:discord.Interaction, employee:discord.Member, hourly_rate:str, reason:str):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.',ephemeral=True)
    if len(reason.strip()) < 5:
        return await interaction.response.send_message('Provide a reason of at least five characters.',ephemeral=True)
    try:
        rate=Decimal(hourly_rate)
        if not rate.is_finite() or rate<=0 or rate>1000:
            raise ValueError('Rate must be greater than $0 and at most $1,000/hour.')
        cents=int((rate*100).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
    except (InvalidOperation,ValueError) as exc:
        return await interaction.response.send_message(f'Invalid rate: {exc}',ephemeral=True)
    async with lock:
        old=streaming_rate_for_employee(interaction.guild_id,employee.id)
        DB.execute('''INSERT INTO employee_stream_rates (guild_id,user_id,rate_cents,set_at,set_by)
                      VALUES (?,?,?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET
                      rate_cents=excluded.rate_cents,set_at=excluded.set_at,set_by=excluded.set_by''',
                   (interaction.guild_id,employee.id,cents,now_iso(),interaction.user.id))
        DB.execute('''INSERT INTO settings_audit(guild_id,actor_id,changed_at,setting,old_value,new_value)
                      VALUES (?,?,?,?,?,?)''',(interaction.guild_id,interaction.user.id,now_iso(),
                      f'streaming_rate:{employee.id}',str(old),str(cents)))
        DB.commit()
    await interaction.response.send_message(
        f'✅ {employee.mention} Streaming rate: {money(old)} → **{money(cents)}/hr**. '
        'Existing shifts are unchanged. If already clocked in, use /switchactive to apply the correct type/rate. '
        f'Reason: {reason}',ephemeral=True)


@bot.tree.command(name='switchactive', description='Manager: switch an active Packing/Streaming shift without losing hours')
@app_commands.describe(employee='Employee with an active shift',work_type='Change current work type',reason='Reason for correction')
@app_commands.guild_only()
@app_commands.choices(work_type=[app_commands.Choice(name='Streaming',value='streaming'),app_commands.Choice(name='Packing',value='packing')])
async def switchactive(interaction:discord.Interaction,employee:discord.Member,work_type:app_commands.Choice[str],reason:str):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.',ephemeral=True)
    if len(reason.strip()) < 5:
        return await interaction.response.send_message('Provide a reason of at least five characters.',ephemeral=True)
    async with lock:
        shift=DB.execute('SELECT * FROM shifts WHERE guild_id=? AND user_id=? AND clock_out IS NULL',
                         (interaction.guild_id,employee.id)).fetchone()
        if not shift:
            return await interaction.response.send_message('Employee has no active shift.',ephemeral=True)
        new_type=work_type.value
        old_type=shift['work_type']
        old_rate=int(shift['hourly_rate_cents'] or (STREAM_RATE_CENTS if old_type=='streaming' else PACK_RATE_CENTS))
        new_rate=shift_rate_for_employee(interaction.guild_id,employee.id,new_type)
        try:
            validate_shift_type_correction(shift['id'],old_type,new_type)
        except ValueError as exc:
            return await interaction.response.send_message(f'❌ {exc}',ephemeral=True)
        if old_type==new_type and old_rate==new_rate:
            return await interaction.response.send_message('Shift is already at the requested type and rate.',ephemeral=True)
        if new_rate==old_rate:
            # Same pay: reclassify in place. Preserve original clock-in and shift ID.
            DB.execute('UPDATE shifts SET work_type=?,hourly_rate_cents=? WHERE id=?',
                       (new_type,new_rate,shift['id']))
            shift_audit(interaction.guild_id,shift['id'],employee.id,'edit',
                        shift['clock_in'],None,shift['clock_in'],None,reason.strip(),interaction.user.id)
            audit_shift_work_type(interaction.guild_id,shift['id'],old_type,new_type,old_rate,new_rate)
            outcome=f'✅ Active shift #{shift["id"]} updated in place: {old_type.title()} → {new_type.title()} at {money(new_rate)}/hr. Original clock-in kept.'
        else:
            # Different pay: close old segment, start next immediately. No overlapping hours.
            cutoff=now_iso()
            DB.execute('UPDATE shifts SET clock_out=? WHERE id=?',(cutoff,shift['id']))
            cursor=DB.execute('''INSERT INTO shifts(guild_id,user_id,clock_in,hourly_rate_cents,work_type)
                                 VALUES (?,?,?,?,?)''',(interaction.guild_id,employee.id,cutoff,new_rate,new_type))
            new_id=cursor.lastrowid
            shift_audit(interaction.guild_id,shift['id'],employee.id,'edit',shift['clock_in'],None,shift['clock_in'],cutoff,reason.strip(),interaction.user.id)
            audit_shift_work_type(interaction.guild_id,shift['id'],old_type,old_type,old_rate,old_rate)
            shift_audit(interaction.guild_id,new_id,employee.id,'add',None,None,cutoff,None,reason.strip(),interaction.user.id)
            audit_shift_work_type(interaction.guild_id,new_id,None,new_type,None,new_rate)
            outcome=f'✅ Old shift #{shift["id"]} closed and shift #{new_id} opened immediately at {money(new_rate)}/hr. No hours lost or overlapped.'
        DB.commit()
    await interaction.response.send_message(outcome+' Review any linked commissions and payroll.',ephemeral=True)
    await notify_shift_correction(interaction, f'🔄 Active shift work type updated by {interaction.user.display_name} for {employee.display_name}: {old_type} → {new_type}. Reason: {reason}')


@bot.tree.command(name='editshift', description='Manager: correct an existing shift (audited)')
@app_commands.describe(shift_id='Shift ID from /shiftlog',clock_in='Start ISO datetime WITH offset',
                       clock_out='End ISO datetime WITH offset',reason='Required explanation',work_type='Streaming or Packing (leave unchanged if omitted)')
@app_commands.guild_only()
@app_commands.choices(work_type=[app_commands.Choice(name='Streaming',value='streaming'),app_commands.Choice(name='Packing',value='packing')])
async def editshift(interaction: discord.Interaction, shift_id: int, clock_in: str, clock_out: str, reason: str, work_type: app_commands.Choice[str]|None=None):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.',ephemeral=True)
        return
    if not 5 <= len(reason.strip()) <= 300:
        await interaction.response.send_message('Reason must contain 5–300 characters.',ephemeral=True)
        return
    try:
        start,end = parse_shift_time(clock_in),parse_shift_time(clock_out)
        async with lock:
            shift=DB.execute('SELECT * FROM shifts WHERE id=? AND guild_id=?',(shift_id,interaction.guild_id)).fetchone()
            if not shift:
                raise ValueError('Shift not found.')
            validate_interval(interaction.guild_id,shift['user_id'],start,end,shift_id)
            chosen_type=work_type.value if work_type is not None else shift['work_type']
            chosen_rate=shift_correction_rate(interaction.guild_id,shift['user_id'],shift['work_type'],shift['hourly_rate_cents'],chosen_type)
            validate_shift_type_correction(shift_id,shift['work_type'],chosen_type)
            # Fixed Character Break pay credits can exceed actual shift duration.
            # Shift corrections remain subject to clock overlap and commission-type checks.
            old_in,old_out=shift['clock_in'],shift['clock_out']
            DB.execute('UPDATE shifts SET clock_in=?,clock_out=?,work_type=?,hourly_rate_cents=? WHERE id=?',
                       (start.isoformat(),end.isoformat(),chosen_type,chosen_rate,shift_id))
            shift_audit(interaction.guild_id,shift_id,shift['user_id'],'edit',old_in,old_out,
                        start.isoformat(),end.isoformat(),reason.strip(),interaction.user.id)
            audit_shift_work_type(interaction.guild_id,shift_id,shift['work_type'],chosen_type,shift['hourly_rate_cents'],chosen_rate)
            DB.commit()
        display_name = await resolve_employee_name(interaction.guild, shift['user_id'])
        await interaction.response.send_message(f'✅ Shift #{shift_id} corrected for {display_name}. '
             f'{(end-start).total_seconds()/3600:.2f} hours • {chosen_type.title()} at {money(chosen_rate)}/hr. Reason: {reason}. '
             'Recheck payroll and approved commissions before payment.',ephemeral=True)
        await notify_shift_correction(interaction,f'📝 **Shift #{shift_id} edited** by {discord.utils.escape_markdown(interaction.user.display_name)} '
             f'for {display_name}. Previous: {old_in} → {old_out or "OPEN"}. '
             f'New: {start.isoformat()} → {end.isoformat()}. Work type: {shift["work_type"]} → {chosen_type}; rate: {money(shift["hourly_rate_cents"])} → {money(chosen_rate)}. Reason: {reason}')
    except ValueError as exc:
        await interaction.response.send_message(f'❌ {exc}',ephemeral=True)


@bot.tree.command(name='addshift', description='Manager: add a missed shift with audit record')
@app_commands.describe(employee='Employee to credit',clock_in='Start ISO datetime WITH offset',
                       clock_out='End ISO datetime WITH offset',reason='Required explanation',work_type='Streaming or Packing')
@app_commands.guild_only()
@app_commands.choices(work_type=[app_commands.Choice(name='Streaming',value='streaming'),app_commands.Choice(name='Packing',value='packing')])
async def addshift(interaction: discord.Interaction, employee: discord.Member, clock_in: str, clock_out: str, reason: str, work_type: app_commands.Choice[str]):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.',ephemeral=True)
        return
    if not 5 <= len(reason.strip()) <= 300:
        await interaction.response.send_message('Reason must contain 5–300 characters.',ephemeral=True)
        return
    try:
        start,end=parse_shift_time(clock_in),parse_shift_time(clock_out)
        async with lock:
            validate_interval(interaction.guild_id,employee.id,start,end)
            rate=shift_rate_for_employee(interaction.guild_id,employee.id,work_type.value)
            cursor=DB.execute('INSERT INTO shifts(guild_id,user_id,clock_in,clock_out,hourly_rate_cents,work_type) VALUES(?,?,?,?,?,?)',
                              (interaction.guild_id,employee.id,start.isoformat(),end.isoformat(),rate,work_type.value))
            sid=cursor.lastrowid
            shift_audit(interaction.guild_id,sid,employee.id,'add',None,None,start.isoformat(),end.isoformat(),reason.strip(),interaction.user.id)
            audit_shift_work_type(interaction.guild_id,sid,None,work_type.value,None,rate)
            DB.commit()
        await interaction.response.send_message(f'✅ Added shift #{sid} for {employee.mention}: '
               f'{(end-start).total_seconds()/3600:.2f} hours • {work_type.value.title()} at {money(rate)}/hr; reason: {reason}.',ephemeral=True)
        await notify_shift_correction(interaction,f'📝 **Shift #{sid} added** for {discord.utils.escape_markdown(employee.display_name)} by '
               f'{discord.utils.escape_markdown(interaction.user.display_name)}. {start.isoformat()} → {end.isoformat()}. {work_type.value.title()} at {money(rate)}/hr. Reason: {reason}')
    except ValueError as exc:
        await interaction.response.send_message(f'❌ {exc}',ephemeral=True)


@bot.tree.command(name='shiftlog', description='Manager: browse past shifts with pages and Packing filter')
@app_commands.describe(employee='Optional employee to search',
                       page='Page number (1 = newest, 2 = older, etc.)',
                       work_type='Optional: show only Packing or Streaming',
                       limit='Shifts per page (up to 20)')
@app_commands.choices(work_type=[
    app_commands.Choice(name='Packing', value='packing'),
    app_commands.Choice(name='Streaming', value='streaming')
])
@app_commands.guild_only()
async def shiftlog(interaction: discord.Interaction, employee: discord.Member | None = None,
                   page: app_commands.Range[int, 1, 100000] = 1,
                   work_type: app_commands.Choice[str] | None = None,
                   limit: app_commands.Range[int, 1, 20] = 15):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    params = [interaction.guild_id]
    where = ' WHERE guild_id=?'
    if employee:
        where += ' AND user_id=?'
        params.append(employee.id)
    if work_type:
        where += ' AND work_type=?'
        params.append(work_type.value)
    total = DB.execute('SELECT COUNT(*) FROM shifts' + where, params).fetchone()[0]
    offset = (page - 1) * limit
    shifts = DB.execute(
        'SELECT id,user_id,clock_in,clock_out,work_type,hourly_rate_cents '
        'FROM shifts' + where + ' ORDER BY id DESC LIMIT ? OFFSET ?',
        [*params, limit, offset]
    ).fetchall()
    if not shifts:
        await interaction.followup.send(
            f'No shifts found on page {page}. Total matching shifts: {total}. '
            'Try an earlier page number or remove filters.', ephemeral=True)
        return
    await preload_employee_names(interaction.guild, [sh['user_id'] for sh in shifts])
    pages = (total + limit - 1) // limit
    heading = (f'**Shift log — page {page}/{pages}** · {total} matching shifts'
               + (f' · {employee_name(interaction.guild, employee.id)}' if employee else '')
               + (f' · {work_type.name}' if work_type else ''))
    lines = [heading]
    for sh in shifts:
        started = boss_local(sh['clock_in'])
        ended = boss_local(sh['clock_out']) if sh['clock_out'] else 'ACTIVE'
        rate = money(sh['hourly_rate_cents']) if sh['hourly_rate_cents'] is not None else 'default'
        lines.append(
            f'`#{sh["id"]}` {discord.utils.escape_markdown(employee_name(interaction.guild, sh["user_id"]))} '
            f'| {started} → {ended} ET | {sh["work_type"].title()} {rate}/hr'
        )
    lines.append(f'**To correct one:** `/fixpackingrate shift_id:<ID> hourly_rate:15.00`')
    if page < pages:
        lines.append(f'**Older shifts:** `/shiftlog page:{page+1}` (keep the same filters)')
    # Long employee names can exceed the Discord message limit; send as multiple messages
    # rather than silently truncating shift entries.
    chunk = ''
    for line in lines:
        if chunk and len(chunk) + len(line) + 1 > 1850:
            await interaction.followup.send(chunk, ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none())
            chunk = ''
        chunk += ('\n' if chunk else '') + line
    if chunk:
        await interaction.followup.send(chunk, ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none())


@bot.tree.command(name='shiftaudit', description='Manager: download complete shift correction history')
@app_commands.guild_only()
async def shiftaudit(interaction:discord.Interaction):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.',ephemeral=True)
        return
    result=DB.execute('SELECT * FROM shift_audit WHERE guild_id=? ORDER BY id',(interaction.guild_id,)).fetchall()
    out=io.StringIO()
    writer=csv.writer(out)
    fields=['id','guild_id','shift_id','user_id','action','old_start','old_end','new_start','new_end','reason','actor_id','created_at','old_work_type','new_work_type','old_rate_cents','new_rate_cents']
    writer.writerow(fields)
    for row in result:
        writer.writerow([row[f] for f in fields])
    await interaction.response.send_message(file=discord.File(io.BytesIO(out.getvalue().encode('utf-8-sig')),
                   filename='etbeast_demo_shift_audit.csv'),ephemeral=True)


@bot.tree.command(name='backup', description='Manager: download a consistent SQLite database backup')
@app_commands.guild_only()
async def backup(interaction:discord.Interaction):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.',ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        target=Path(tmp)/'etbeast_demo-backup.sqlite3'
        with sqlite3.connect(target) as output:
            async with lock:
                DB.backup(output)
        if target.stat().st_size>24*1024*1024:
            await interaction.followup.send('Database exceeds Discord upload limit. Back up the data folder directly.',ephemeral=True)
            return
        await interaction.followup.send('Private database backup. Also back up `data/proofs` separately and keep copies secure.',
                 file=discord.File(str(target),filename='etbeast_demo-backup.sqlite3'),ephemeral=True)


@bot.tree.command(name='fine', description='Manager: record a proposed fine/incident (NOT deducted from pay)')
@app_commands.describe(employee='Employee involved', amount='Proposed USD amount for review',
                       reason='Specific incident and explanation', event_date='Event date YYYY-MM-DD')
@app_commands.guild_only()
async def fine(interaction: discord.Interaction, employee: discord.Member, amount: str,
               reason: str, event_date: str):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    try:
        dollars = parse_nonnegative(amount, '100000')
        cents = int((dollars * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
        if cents < 1:
            raise ValueError
        parsed_date = datetime.strptime(event_date, '%Y-%m-%d').date()
        if parsed_date > datetime.now(timezone.utc).date():
            raise ValueError
    except ValueError:
        await interaction.response.send_message('Enter a positive amount and a valid past/present date in YYYY-MM-DD.', ephemeral=True)
        return
    reason = reason.strip()
    if not reason or len(reason) > 250:
        await interaction.response.send_message('Reason must be 1–250 characters.', ephemeral=True)
        return
    async with lock:
        cursor = DB.execute("""INSERT INTO incident_fines
             (guild_id,employee_id,proposed_amount_cents,reason,event_date,created_at,created_by)
             VALUES(?,?,?,?,?,?,?)""",
             (interaction.guild_id,employee.id,cents,reason,event_date,now_iso(),interaction.user.id))
        incident_id = cursor.lastrowid
        DB.execute("""INSERT INTO incident_fine_audit
            (guild_id,incident_id,action,actor_id,occurred_at,note) VALUES(?,?,?,?,?,?)""",
            (interaction.guild_id,incident_id,'created',interaction.user.id,now_iso(),reason))
        DB.commit()
    await interaction.response.send_message(
        f'Incident #{incident_id} recorded for {employee.mention}: proposed {money(cents)}. '
        '**Status: pending review. No wages or commissions were deducted.**', ephemeral=True)
    try:
        await employee.send(f'ETBeastDemo recorded an incident for review (#{incident_id}, event date {event_date}). '
                            f'Reason: {reason}. Proposed amount: {money(cents)}. '
                            'This record does not deduct wages; contact management to dispute details.')
    except (discord.Forbidden, discord.HTTPException):
        pass


@bot.tree.command(name='fines', description='Manager: inspect proposed fines and incident statuses')
@app_commands.describe(employee='Optional employee filter', days='Lookback days (default 30)')
@app_commands.guild_only()
async def fines(interaction: discord.Interaction, employee: discord.Member | None = None,
                days: app_commands.Range[int, 1, 365] = 30):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    query = 'SELECT * FROM incident_fines WHERE guild_id=? AND created_at>=?'
    params = [interaction.guild_id, cutoff]
    if employee:
        query += ' AND employee_id=?'
        params.append(employee.id)
    query += ' ORDER BY id DESC LIMIT 20'
    rows = DB.execute(query, params).fetchall()
    if not rows:
        await interaction.response.send_message('No incident records found.', ephemeral=True)
        return
    await preload_employee_names(interaction.guild, [r['employee_id'] for r in rows])
    lines = [f'#{r["id"]} • {employee_name(interaction.guild,r["employee_id"])} • {money(r["proposed_amount_cents"])} • '
             f'{("deducted week "+active_deductions[r["id"]]) if r["id"] in active_deductions else r["status"]} • {r["event_date"]} • {discord.utils.escape_mentions(r["reason"][:55])}'
             for r in rows]
    await interaction.response.send_message('**Incidents and approved deductions:**\n' + '\n'.join(lines), ephemeral=True)


@bot.tree.command(name='reviewfine', description='Manager: review, dismiss, or authorize a payroll deduction')
@app_commands.describe(fine_id='ID shown in /fines', status='Review, dismiss, or approve an authorized deduction',
                       note='Review decision or correction explanation')
@app_commands.choices(status=[app_commands.Choice(name='Reviewed (record only)', value='reviewed'),
                             app_commands.Choice(name='Approve authorized payroll deduction',value='deduct'),
                             app_commands.Choice(name='Dismissed', value='dismissed')])
@app_commands.guild_only()
async def reviewfine(interaction: discord.Interaction, fine_id: int,
                     status: app_commands.Choice[str], note: str):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    note = note.strip()
    if not note or len(note) > 250:
        await interaction.response.send_message('Provide a review note of 1–250 characters.', ephemeral=True)
        return
    async with lock:
        row = DB.execute('SELECT * FROM incident_fines WHERE guild_id=? AND id=?',
                         (interaction.guild_id, fine_id)).fetchone()
        if not row:
            await interaction.response.send_message('Incident ID not found.', ephemeral=True)
            return
        if DB.execute('SELECT 1 FROM fine_payroll_deductions WHERE guild_id=? AND incident_id=?',
                      (interaction.guild_id,fine_id)).fetchone():
            await interaction.response.send_message('This fine has an authorized payroll deduction and cannot be dismissed or applied twice.',ephemeral=True)
            return
        if row['status'] == 'dismissed':
            await interaction.response.send_message('Dismissed incidents cannot be reopened; create a new record if needed.', ephemeral=True)
            return
        if status.value == 'deduct':
            # A deduction must be individually authorized and must never modify
            # a finalized or partially paid period. `note` documents authorization.
            if len(note) < 12 or not note.lower().startswith('authorized:'):
                await interaction.response.send_message(
                    'To approve a deduction, enter a note starting with "Authorized:" '
                    'followed by the written authorization reference and explanation. '
                    'Verify your legal authority first.',ephemeral=True)
                return
            week_start=payroll_week_start(parse_commission_finish(row['event_date']+' 12:00')).isoformat()
            if datetime.now(timezone.utc) >= payroll_week_bounds(week_start)[1]:
                await interaction.response.send_message('This pay period has ended. Do not backdate deductions through this command.',ephemeral=True)
                return
            frozen=payroll_week_status(interaction.guild_id,week_start)
            paid=employee_payment_totals(interaction.guild_id,week_start)
            # Never silently discard part of a fine or produce a negative payable balance.
            this_week=payroll_week_data(interaction.guild_id,week_start)
            current=this_week['employees'].get(str(row['employee_id']),{})
            remaining=max(0,current.get('gross',0)-current.get('fine_deductions',0))
            if row['proposed_amount_cents'] > remaining:
                await interaction.response.send_message(
                    f'This deduction would exceed currently recorded gross earnings remaining '
                    f'({money(remaining)}). Record earnings first, or do not apply this deduction.',ephemeral=True)
                return
            if frozen is not None or any(v > 0 for v in paid.values()):
                await interaction.response.send_message(
                    'Payroll has already been approved or has payments recorded for this week. '
                    'Use a separately documented adjustment outside this command.',ephemeral=True)
                return
            if DB.execute('SELECT 1 FROM fine_payroll_deductions WHERE guild_id=? AND incident_id=?',
                          (interaction.guild_id,fine_id)).fetchone():
                await interaction.response.send_message('This fine was already applied to payroll.',ephemeral=True)
                return
            DB.execute('''INSERT INTO fine_payroll_deductions
                (guild_id,incident_id,employee_id,week_start,amount_cents,authorization_reference,approved_by,approved_at)
                VALUES (?,?,?,?,?,?,?,?)''',
                (interaction.guild_id,fine_id,row['employee_id'],week_start,
                 row['proposed_amount_cents'],note,interaction.user.id,now_iso()))
            DB.execute('UPDATE incident_fines SET status=?,updated_at=?,updated_by=?,resolution_note=? WHERE id=?',
                       ('reviewed',now_iso(),interaction.user.id,note,fine_id))
            DB.execute('''INSERT INTO incident_fine_audit
                (guild_id,incident_id,action,actor_id,occurred_at,note) VALUES(?,?,?,?,?,?)''',
                (interaction.guild_id,fine_id,'deduction_approved',interaction.user.id,now_iso(),note))
            DB.commit()
            await interaction.response.send_message(
                f'Fine #{fine_id}: authorized deduction of {money(row["proposed_amount_cents"])} '
                f'will be deducted automatically from the {week_start} pay period payable balance. '
                'It appears separately on weekly statements. Gross earnings remain unchanged.',ephemeral=True)
            return
        DB.execute("""UPDATE incident_fines SET status=?,updated_at=?,updated_by=?,resolution_note=? WHERE id=?""",
                   (status.value,now_iso(),interaction.user.id,note,fine_id))
        DB.execute("""INSERT INTO incident_fine_audit
            (guild_id,incident_id,action,actor_id,occurred_at,note) VALUES(?,?,?,?,?,?)""",
            (interaction.guild_id,fine_id,status.value,interaction.user.id,now_iso(),note))
        DB.commit()
    await interaction.response.send_message(
        f'Incident #{fine_id} marked **{status.value}**. Pay remains unchanged. ', ephemeral=True)


@bot.tree.command(name='finescsv', description='Manager: export incident and fine proposals (separate from payroll)')
@app_commands.describe(days='Lookback days, default 30')
@app_commands.guild_only()
async def finescsv(interaction: discord.Interaction, days: app_commands.Range[int, 1, 365] = 30):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = DB.execute("""SELECT * FROM incident_fines WHERE guild_id=? AND created_at>=?
                         ORDER BY id""", (interaction.guild_id, cutoff)).fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    headers=['id','employee_id','proposed_amount_usd','reason','event_date','status',
             'created_at','created_by','updated_at','updated_by','resolution_note','deducted_week']
    approval_week={r['incident_id']:r['week_start'] for r in DB.execute(
        'SELECT incident_id,week_start FROM fine_payroll_deductions WHERE guild_id=?',(interaction.guild_id,)).fetchall()}
    writer.writerow(headers)
    for r in rows:
        writer.writerow([r['id'],r['employee_id'],money(r['proposed_amount_cents']),r['reason'],
                         r['event_date'],r['status'],r['created_at'],r['created_by'],
                         r['updated_at'],r['updated_by'],r['resolution_note'],approval_week.get(r['id'],'')])
    await interaction.response.send_message(
        '**Incident ledger — includes approved deductions by pay period.**',
        file=discord.File(io.BytesIO(output.getvalue().encode('utf-8-sig')),
                          filename='etbeast_demo_incidents.csv'), ephemeral=True)


@bot.tree.command(name='payroll', description='Manager: export completed-shift wages and approved commission as CSV')
@app_commands.describe(days='Number of calendar days to include, 1–90 (defaults to 7)')
@app_commands.guild_only()
async def payroll(interaction: discord.Interaction, days: app_commands.Range[int, 1, 90] = 7):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.', ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = DB.execute('''SELECT s.id,s.user_id,s.clock_in,s.clock_out,s.hourly_rate_cents,
        s.work_type,
        COALESCE(SUM(CASE WHEN c.status='approved' THEN c.commission_cents ELSE 0 END),0) AS commission,
        COALESCE(SUM(CASE WHEN c.status='approved' THEN c.guarantee_topup_cents ELSE 0 END),0) AS topup
        FROM shifts s LEFT JOIN commissions c ON c.shift_id=s.id
        WHERE s.guild_id=? AND s.clock_out IS NOT NULL AND s.clock_out>=?
        GROUP BY s.id ORDER BY s.clock_out''', (interaction.guild_id, cutoff)).fetchall()
    await preload_employee_names(interaction.guild, [r['user_id'] for r in rows] + [r['employee_id'] for r in DB.execute('SELECT employee_id FROM bonuses WHERE guild_id=? AND created_at>=? AND voided_at IS NULL', (interaction.guild_id,cutoff))])
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['record_type','record_id','discord_user_id','employee_nickname','clock_in_utc','clock_out_or_awarded_utc',
                     'hours','base_pay','approved_commission','character_guarantee_topup','bonus','gross_pay','bonus_reason','work_type'])
    total = 0
    for r in rows:
        hours = Decimal(str((datetime.fromisoformat(r['clock_out']) - datetime.fromisoformat(r['clock_in'])).total_seconds())) / Decimal('3600')
        hourly_cents = r['hourly_rate_cents'] if r['hourly_rate_cents'] is not None else HOURLY_RATE_CENTS
        base = int((hours * hourly_cents).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
        gross = base + r['commission'] + r['topup']
        total += gross
        writer.writerow(['shift', r['id'], r['user_id'],employee_name(interaction.guild,r['user_id']),r['clock_in'],r['clock_out'],
                         f'{hours:.4f}',money(base),money(r['commission']),money(r['topup']),money(0),money(gross),'',r['work_type']])
    bonus_rows = DB.execute('''SELECT id,employee_id,amount_cents,reason,created_at
                            FROM bonuses WHERE guild_id=? AND created_at>=? AND voided_at IS NULL
                            ORDER BY created_at''', (interaction.guild_id, cutoff)).fetchall()
    bonus_total = 0
    for bonus_row in bonus_rows:
        cents = bonus_row['amount_cents']
        bonus_total += cents
        writer.writerow(['bonus',bonus_row['id'],bonus_row['employee_id'],employee_name(interaction.guild,bonus_row['employee_id']),'',bonus_row['created_at'],
                         '',money(0),money(0),money(0),money(cents),money(cents),bonus_row['reason'],'bonus'])
    total += bonus_total
    content = io.BytesIO(output.getvalue().encode('utf-8-sig'))
    await interaction.followup.send(
        f'{len(rows)} completed shifts and {len(bonus_rows)} active bonuses in the last {days} days. '
        f'Bonuses: **{money(bonus_total)}**. Calculated total: **{money(total)}**. '
        '**Review before payroll**: this prototype does not calculate overtime, taxes, or breaks. '
        'Bonuses are dated by award time, not assigned to a pay period. Character Break top-ups require screenshot verification. Legacy shifts keep saved historical rates. '
        '**Weekly manual hours are excluded from this rolling CSV. Use /weeklypayroll → Week CSV for complete pay.**',
        file=discord.File(content, filename='etbeast_demo_payroll.csv'), ephemeral=True)



# V18: manager-only payroll review, using the same rolling cutoff/earning rules as /payroll CSV.
def payroll_review_data(guild_id, days):
    cutoff=(datetime.now(timezone.utc)-timedelta(days=days)).isoformat()
    shifts=DB.execute("""SELECT s.id,s.user_id,s.clock_in,s.clock_out,s.hourly_rate_cents,s.work_type,
      COALESCE(SUM(CASE WHEN c.status='approved' THEN c.commission_cents ELSE 0 END),0) AS commission,
      COALESCE(SUM(CASE WHEN c.status='approved' THEN c.guarantee_topup_cents ELSE 0 END),0) AS topup
      FROM shifts s LEFT JOIN commissions c ON c.shift_id=s.id
      WHERE s.guild_id=? AND s.clock_out IS NOT NULL AND s.clock_out>=?
      GROUP BY s.id ORDER BY s.clock_out""",(guild_id,cutoff)).fetchall()
    bonuses=DB.execute("""SELECT id,employee_id,amount_cents,reason,created_at FROM bonuses
      WHERE guild_id=? AND created_at>=? AND voided_at IS NULL ORDER BY created_at""",(guild_id,cutoff)).fetchall()
    pending=DB.execute("""SELECT user_id,COUNT(*) AS n FROM commissions
      WHERE guild_id=? AND status='pending' GROUP BY user_id""",(guild_id,)).fetchall()
    active=DB.execute("""SELECT user_id,COUNT(*) AS n FROM shifts
      WHERE guild_id=? AND clock_out IS NULL GROUP BY user_id""",(guild_id,)).fetchall()
    totals={}
    def entry(uid):
        return totals.setdefault(uid,dict(hours=Decimal('0'),stream_hours=Decimal('0'),packing_hours=Decimal('0'),
            other_hours=Decimal('0'),base=0,commission=0,topup=0,bonus=0,shift_count=0,bonus_items=[],shifts=[]))
    for r in shifts:
        hours=Decimal(str((datetime.fromisoformat(r['clock_out'])-datetime.fromisoformat(r['clock_in'])).total_seconds()))/Decimal('3600')
        hourly=int(r['hourly_rate_cents'] if r['hourly_rate_cents'] is not None else HOURLY_RATE_CENTS)
        base=int((hours*hourly).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
        e=entry(int(r['user_id']))
        e['hours']+=hours
        kind=r['work_type'] if r['work_type'] in ('streaming','packing') else 'other'
        e[{'streaming':'stream_hours','packing':'packing_hours','other':'other_hours'}[kind]]+=hours
        e['base']+=base;e['commission']+=int(r['commission']);e['topup']+=int(r['topup']);e['shift_count']+=1
        e['shifts'].append((r['id'],kind,base,int(r['commission']),int(r['topup'])))
    for r in bonuses:
        e=entry(int(r['employee_id']))
        e['bonus']+=int(r['amount_cents'])
        e['bonus_items'].append((r['id'],int(r['amount_cents']),r['reason']))
    pending_counts={int(r['user_id']):int(r['n']) for r in pending}
    active_counts={int(r['user_id']):int(r['n']) for r in active}
    return totals,pending_counts,active_counts


def payroll_review_embed(guild,days,totals,pending,active,employee_id=None):
    embed=discord.Embed(title=f'💵 ETBeastDemo Payroll Review · Last {days} days',
      description='**Estimated gross earnings — not final payroll.** Completed shifts by clock-out date and bonuses by award date.',
      color=discord.Color.blurple())
    if employee_id is None:
        amounts={uid:r['base']+r['commission']+r['topup']+r['bonus'] for uid,r in totals.items()}
        summary={k:sum(r[k] for r in totals.values()) for k in ('base','commission','topup','bonus')}
        gross=sum(amounts.values())
        hours=sum((r['hours'] for r in totals.values()),Decimal(0))
        embed.add_field(name='Employees / completed hours',value=f'{len(totals)} employees · {hours:.2f} hr',inline=False)
        embed.add_field(name='Hourly wages',value=money(summary['base']))
        embed.add_field(name='Commissions / saved rates',value=money(summary['commission']))
        embed.add_field(name='Character Break top-ups',value=money(summary['topup']))
        embed.add_field(name='Bonuses',value=money(summary['bonus']))
        embed.add_field(name='Estimated gross total',value=f'**{money(gross)}**',inline=False)
        if amounts:
            leaders=sorted(amounts,key=lambda uid:(-amounts[uid],uid))
            lines=[f'**{employee_name(guild,uid)}** — {money(amounts[uid])} · {totals[uid]["hours"]:.2f} hr' for uid in leaders[:12]]
            if len(leaders)>12: lines.append(f'…and {len(leaders)-12} more. Select an employee below.')
            embed.add_field(name='Employee totals',value='\n'.join(lines),inline=False)
        else:
            embed.add_field(name='No completed shifts or bonuses',value='Choose another reporting window.',inline=False)
        if pending or active:
            embed.add_field(name='⚠️ Requires attention',value=f'{sum(active.values())} open shifts · {sum(pending.values())} pending commission submissions (all dates)',inline=False)
    else:
        e=totals.get(employee_id)
        embed.title=f'💵 {employee_name(guild,employee_id)} · Last {days} days'
        if e is None:
            embed.description='No completed shifts or bonuses recorded in this window.'
        else:
            gross=e['base']+e['commission']+e['topup']+e['bonus']
            embed.add_field(name='Shifts',value=f'{e["shift_count"]} completed · {e["hours"]:.2f} hr',inline=False)
            embed.add_field(name='Streaming hours',value=f'{e["stream_hours"]:.2f} hr')
            embed.add_field(name='Packing hours',value=f'{e["packing_hours"]:.2f} hr')
            if e['other_hours']:
                embed.add_field(name='Historical/other hours',value=f'{e["other_hours"]:.2f} hr')
            embed.add_field(name='Base wages',value=money(e['base']))
            embed.add_field(name='Approved commissions',value=money(e['commission']))
            embed.add_field(name='Character Break top-ups',value=money(e['topup']))
            embed.add_field(name='Bonuses',value=money(e['bonus']))
            embed.add_field(name='**Estimated gross**',value=f'**{money(gross)}**',inline=False)
            if e['bonus_items']:
                embed.add_field(name='Bonuses recorded',value='\n'.join(f'#{bid} · {money(amt)} · {discord.utils.escape_markdown(reason[:60])}' for bid,amt,reason in e['bonus_items'][-5:]),inline=False)
        flags=[]
        if active.get(employee_id):flags.append('Open shift: hours not included yet')
        if pending.get(employee_id):flags.append(f'{pending[employee_id]} pending revenue submission(s)')
        if flags:embed.add_field(name='⚠️ Check before paying',value='\n'.join(flags),inline=False)
    embed.set_footer(text='Rolling UTC window · No overtime, tax, break, net-pay or paid/unpaid tracking. Verify payroll outside Discord.')
    return embed


class PayrollReviewView(discord.ui.View):
    def __init__(self,owner_id,guild_id,days=7,employee_id=None,totals=None,pending=None,active=None):
        super().__init__(timeout=600)
        self.owner_id=owner_id;self.guild_id=guild_id;self.days=days;self.employee_id=employee_id
        self.totals=totals or {};self.pending=pending or {};self.active=active or {}
        options=[discord.SelectOption(label=f'Last {d} days',value=str(d),default=d==days) for d in (7,14,30,60,90)]
        date_select=discord.ui.Select(placeholder='Reporting window',options=options,row=0)
        async def choose_days(interaction):
            self.days=int(date_select.values[0]);self.employee_id=None
            await self.redraw(interaction)
        date_select.callback=choose_days;self.add_item(date_select)
        employee_options=[discord.SelectOption(label='All employees',value='all',default=employee_id is None)]
        for uid,e in sorted(self.totals.items(),key=lambda kv:kv[0])[:24]:
            # Nicknames are preloaded by the sending code; keep labels under Discord limit.
            label=self.employee_labels.get(uid,str(uid)) if hasattr(self,'employee_labels') else str(uid)
            employee_options.append(discord.SelectOption(label=label[:100],value=str(uid),default=uid==employee_id))
        emp_select=discord.ui.Select(placeholder='Select employee',options=employee_options,row=1)
        async def choose_employee(interaction):
            self.employee_id=None if emp_select.values[0]=='all' else int(emp_select.values[0])
            await self.redraw(interaction)
        emp_select.callback=choose_employee;self.add_item(emp_select)

    async def interaction_check(self,interaction):
        if interaction.user.id!=self.owner_id or interaction.guild_id!=self.guild_id or not manager(interaction) or not allowed_guild(interaction):
            await interaction.response.send_message('Manager-only. Open your own /dashboard.',ephemeral=True)
            return False
        return True

    async def redraw(self,interaction):
        totals,pending,active=payroll_review_data(self.guild_id,self.days)
        await preload_employee_names(interaction.guild,list(set(totals)|set(pending)|set(active)))
        if self.employee_id is not None and self.employee_id not in totals and self.employee_id not in pending and self.employee_id not in active:
            self.employee_id=None
        view=make_payroll_view(self.owner_id,interaction.guild,self.days,self.employee_id,totals,pending,active)
        await interaction.response.edit_message(embed=payroll_review_embed(interaction.guild,self.days,totals,pending,active,self.employee_id),view=view)

    @discord.ui.button(label='Refresh',emoji='🔄',style=discord.ButtonStyle.secondary,row=2)
    async def refresh(self,interaction,button):
        await self.redraw(interaction)

    @discord.ui.button(label='Download CSV',emoji='📄',style=discord.ButtonStyle.primary,row=2)
    async def export(self,interaction,button):
        # Preserve the current payroll exporter rather than reimplement calculations.
        await run_boss_command(interaction,'payroll',days=self.days)


def make_payroll_view(owner_id,guild,days,employee_id,totals,pending,active):
    view=PayrollReviewView(owner_id,guild.id,days,employee_id,totals,pending,active)
    # Update member labels after construction to show nicknames in menu.
    for child in view.children:
        if isinstance(child,discord.ui.Select) and child.placeholder=='Select employee':
            for option in child.options:
                if option.value!='all':option.label=employee_name(guild,int(option.value))[:100]
    return view


@bot.tree.command(name='payrollreview',description='Manager: view employee payroll estimates in Discord')
@app_commands.guild_only()
async def payrollreview(interaction: discord.Interaction):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.',ephemeral=True);return
    await interaction.response.defer(ephemeral=True)
    totals,pending,active=payroll_review_data(interaction.guild_id,7)
    await preload_employee_names(interaction.guild,list(set(totals)|set(pending)|set(active)))
    await interaction.followup.send(embed=payroll_review_embed(interaction.guild,7,totals,pending,active),
        view=make_payroll_view(interaction.user.id,interaction.guild,7,None,totals,pending,active),ephemeral=True)




# V23: exact Eastern-time Sunday–Saturday payroll periods and review/approval ledger.
# Completed shifts are assigned by clock-out instant; cross-boundary shifts are
# flagged for manual payroll review rather than silently split or assumed correct.
PAYROLL_TZ = ZoneInfo('America/New_York')


def payroll_week_start(moment):
    """Sunday start in New York, handling EST/EDT transitions."""
    if isinstance(moment, str):
        moment = datetime.fromisoformat(moment)
    if isinstance(moment, date) and not isinstance(moment, datetime):
        d = moment
    else:
        d = moment.astimezone(PAYROLL_TZ).date()
    return d - timedelta(days=(d.weekday() + 1) % 7)


def parse_commission_finish(value):
    """Human-entered completion time, Eastern; rejects ambiguous DST fall-back hour."""
    try:
        naive = datetime.strptime(value.strip(), '%Y-%m-%d %H:%M')
    except (ValueError, AttributeError) as exc:
        raise ValueError('Finished at ET must be YYYY-MM-DD HH:MM (24-hour Eastern).') from exc
    local = naive.replace(tzinfo=PAYROLL_TZ)
    if local.astimezone(timezone.utc).astimezone(PAYROLL_TZ).replace(tzinfo=None) != naive:
        raise ValueError('Completion time does not exist in Eastern Time due to the daylight-saving change.')
    other = naive.replace(tzinfo=PAYROLL_TZ,fold=1)
    if local.utcoffset() != other.utcoffset():
        raise ValueError('This Eastern time is ambiguous due to daylight saving. Ask a manager to record completion time.')
    return local.astimezone(timezone.utc)


def payroll_week_bounds(start):
    if isinstance(start,str): start=date.fromisoformat(start)
    if start.weekday()!=6: raise ValueError('Payroll weeks must start on Sunday.')
    begin=datetime.combine(start,time.min,tzinfo=PAYROLL_TZ)
    end=datetime.combine(start+timedelta(days=7),time.min,tzinfo=PAYROLL_TZ)
    return begin.astimezone(timezone.utc),end.astimezone(timezone.utc)


def assigned_commission_week(guild_id, row):
    """Return a commission's permanent payroll week, never changing it at approval.

    Modern commissions are pinned at commission approval. For legacy records
    without payroll_week_start, use their original completion week consistently.
    Previously an approved week made the same legacy commission jump to the
    next unpaid week, instantly reducing already-approved gross earnings.
    Legacy additions/edits after approval will change the payroll fingerprint
    and therefore require review rather than silently jumping pay periods.
    """
    if row['payroll_week_start']:
        return row['payroll_week_start']
    finish = row['finished_at'] or row['clock_out'] or row['submitted_at']
    return payroll_week_start(finish).isoformat()


def approved_fine_deductions(guild_id, week_start):
    """Authorized, explicitly approved deductions only; never legacy incidents."""
    rows=DB.execute('SELECT employee_id, SUM(amount_cents) AS amount FROM fine_payroll_deductions WHERE guild_id=? AND week_start=? GROUP BY employee_id',(guild_id,str(week_start))).fetchall()
    return {str(r['employee_id']):int(r['amount']) for r in rows}


def payable_cents(employee_row):
    return max(0, employee_row.get('gross',0)-employee_row.get('fine_deductions',0))


def payroll_week_data(guild_id,start):
    if isinstance(start,str): start=date.fromisoformat(start)
    low,high=payroll_week_bounds(start)
    low_s,high_s=low.isoformat(),high.isoformat()
    # Include intersecting closed shifts and split their base pay at the Sunday cutoff.
    shifts=DB.execute("""SELECT id,user_id,clock_in,clock_out,hourly_rate_cents,work_type FROM shifts
      WHERE guild_id=? AND clock_out IS NOT NULL AND clock_in<? AND clock_out>?
      ORDER BY clock_in,id""",(guild_id,high_s,low_s)).fetchall()
    bonuses=DB.execute("""SELECT id,employee_id,amount_cents,reason,created_at FROM bonuses
      WHERE guild_id=? AND created_at>=? AND created_at<? AND voided_at IS NULL ORDER BY id""",
      (guild_id,low_s,high_s)).fetchall()
    all_commissions=DB.execute("""SELECT c.*,s.clock_in,s.clock_out FROM commissions c
       JOIN shifts s ON s.id=c.shift_id WHERE c.guild_id=? ORDER BY c.id""",(guild_id,)).fetchall()
    included_commissions=[r for r in all_commissions if assigned_commission_week(guild_id,r)==start.isoformat()]
    pending=[dict(id=r['id'],shift_id=r['shift_id'],user_id=r['user_id']) for r in included_commissions if r['status']=='pending']
    # A still-open shift overlapping this week is an approval blocker, not an assumption of zero hours.
    open_shifts=DB.execute("""SELECT id,user_id,clock_in FROM shifts WHERE guild_id=?
      AND clock_out IS NULL AND clock_in<? ORDER BY clock_in""",(guild_id,high_s)).fetchall()
    # Unlike v22, cross-week shifts are permitted: each week receives its own overlap.
    boundary=[]
    employees={}
    def emp(uid):
        return employees.setdefault(str(uid),dict(hours='0',stream_hours='0',packing_hours='0',
            base=0,commission=0,topup=0,bonus=0,shift_ids=[],bonus_ids=[]))
    shift_state=[]
    for r in shifts:
        actual_begin=datetime.fromisoformat(r['clock_in'])
        actual_end=datetime.fromisoformat(r['clock_out'])
        begin=max(actual_begin,low);end=min(actual_end,high)
        if begin>=end: continue
        duration=Decimal(str((end-begin).total_seconds()))/Decimal(3600)
        pay_rate=int(r['hourly_rate_cents'] if r['hourly_rate_cents'] is not None else HOURLY_RATE_CENTS)
        base=int((duration*pay_rate).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
        e=emp(r['user_id'])
        e['hours']=str(Decimal(e['hours'])+duration)
        if r['work_type']=='streaming': e['stream_hours']=str(Decimal(e['stream_hours'])+duration)
        if r['work_type']=='packing': e['packing_hours']=str(Decimal(e['packing_hours'])+duration)
        e['base']+=base;e['shift_ids'].append(r['id'])
        shift_state.append(dict(r))
    manual_state=[]
    for r in manual_weekly_hours_rows(guild_id,start.isoformat()):
        e=emp(r['employee_id'])
        duration=Decimal(r['hours_decimal'])
        e['hours']=str(Decimal(e['hours'])+duration)
        hour_key='stream_hours' if r['work_type']=='streaming' else 'packing_hours'
        e[hour_key]=str(Decimal(e[hour_key])+duration)
        e['manual_hours']=str(Decimal(e.get('manual_hours','0'))+duration)
        e['manual_base']=e.get('manual_base',0)+int(r['wages_cents'])
        e['base']+=int(r['wages_cents'])
        e.setdefault('manual_ids',[]).append(r['id'])
        manual_state.append(dict(r))
    for r in included_commissions:
        if r['status']=='approved':
            e=emp(r['user_id'])
            e['commission']+=int(r['commission_cents']);e['topup']+=int(r['guarantee_topup_cents'] or 0)
    for r in bonuses:
        e=emp(r['employee_id']);e['bonus']+=int(r['amount_cents']);e['bonus_ids'].append(r['id'])
    # Inline query so historical extracted-function payroll test harnesses also work.
    try:
        fine_rows=DB.execute('SELECT employee_id, SUM(amount_cents) amount FROM fine_payroll_deductions WHERE guild_id=? AND week_start=? GROUP BY employee_id',
                             (guild_id,start.isoformat())).fetchall()
        deductions={str(r['employee_id']):int(r['amount']) for r in fine_rows}
    except Exception as exc:
        if 'no such table: fine_payroll_deductions' not in str(exc): raise
        deductions={}
    for uid, amount in deductions.items():
        emp(int(uid))['fine_deductions']=amount
    for e in employees.values():
        e['gross']=e['base']+e['commission']+e['topup']+e['bonus']
        e['fine_deductions']=e.get('fine_deductions',0)
        e['payable']=max(0,e['gross']-e['fine_deductions'])
    commission_state=[dict(r) for r in included_commissions]
    bonus_state=DB.execute("""SELECT id,employee_id,amount_cents,created_at,voided_at,reason FROM bonuses
      WHERE guild_id=? AND created_at>=? AND created_at<? ORDER BY id""",(guild_id,low_s,high_s)).fetchall()
    try:
        deduction_state=[dict(r) for r in DB.execute(
            'SELECT * FROM fine_payroll_deductions WHERE guild_id=? AND week_start=? ORDER BY id',
            (guild_id,start.isoformat())).fetchall()]
    except Exception as exc:
        if 'no such table: fine_payroll_deductions' not in str(exc): raise
        deduction_state=[]
    state={'fine_deductions':deduction_state,'shifts':shift_state,'commissions':commission_state,
      'bonuses':[dict(r) for r in bonus_state],'open':[dict(r) for r in open_shifts]}
    if manual_state:
        state['manual_weekly_hours']=manual_state  # Preserve legacy fingerprints for all weeks without manual hours.
    fingerprint=hashlib.sha256(json.dumps(state,sort_keys=True,default=str).encode()).hexdigest()
    gross=sum(e['gross'] for e in employees.values())
    return dict(start=start.isoformat(),end=(start+timedelta(days=6)).isoformat(),
      employees=employees,gross=gross,payable=sum(e['payable'] for e in employees.values()),pending=pending,open=[dict(r) for r in open_shifts],
      boundary=boundary,fingerprint=fingerprint)


def manual_weekly_hours_rows(guild_id, week_start):
    try:
        return DB.execute("""SELECT * FROM manual_weekly_hours
            WHERE guild_id=? AND week_start=? AND voided_at IS NULL ORDER BY id""",
            (guild_id,str(week_start))).fetchall()
    except Exception as exc:
        # Backward-compatible offline test fixtures without the v53 schema.
        if 'no such table: manual_weekly_hours' not in str(exc): raise
        return []


def parse_manual_weekly_hours(value):
    try:
        amount=Decimal(value.strip())
    except (InvalidOperation, ValueError, AttributeError):
        raise ValueError('Hours must be positive decimal hours, e.g. 4.3 or 23.75.')
    if not amount.is_finite() or amount <= 0 or amount > Decimal('168') or amount.as_tuple().exponent < -2:
        raise ValueError('Enter 0.01–168 decimal hours, with at most two digits after the decimal.')
    return amount


def insert_manual_weekly_hours(guild_id,employee_id,week_start,work_type,hours,reason,actor_id):
    # Caller must hold lock. Recheck finalization inside insertion transaction.
    payroll_week_bounds(week_start)
    if payroll_week_status(guild_id,week_start):
        raise ValueError('This week is already approved/paid. Do not edit finalized payroll.')
    if work_type not in ('streaming','packing'):
        raise ValueError('Choose Streaming or Packing.')
    hours=parse_manual_weekly_hours(str(hours))
    reason=reason.strip()
    if not reason or len(reason)>250:
        raise ValueError('Reason must be 1–250 characters.')
    if DB.execute("""SELECT id FROM manual_weekly_hours WHERE guild_id=? AND employee_id=?
        AND week_start=? AND work_type=? AND voided_at IS NULL""",
        (guild_id,employee_id,str(week_start),work_type)).fetchone():
        raise ValueError('Manual hours already exist for this employee, week and work type. Void the old entry before replacing it.')
    live=payroll_week_data(guild_id,week_start)
    current=Decimal(live['employees'].get(str(employee_id),{}).get('hours','0'))
    if current + hours > Decimal('168'):
        raise ValueError('These hours plus existing clocked/manual hours exceed 168 for the week. Enter only missing hours.')
    rate=shift_rate_for_employee(guild_id,employee_id,work_type)
    wages=int((hours*Decimal(rate)).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
    c=DB.execute("""INSERT INTO manual_weekly_hours
        (guild_id,employee_id,week_start,work_type,hours_decimal,rate_cents,wages_cents,reason,created_at,created_by)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (guild_id,employee_id,str(week_start),work_type,str(hours),rate,wages,reason,now_iso(),actor_id))
    DB.commit()
    return c.lastrowid,wages,rate


def void_manual_weekly_hours(guild_id,entry_id,actor_id,reason):
    reason=reason.strip()
    if not reason or len(reason)>250:
        raise ValueError('Provide a void reason (1–250 characters).')
    row=DB.execute('SELECT * FROM manual_weekly_hours WHERE id=? AND guild_id=?',(entry_id,guild_id)).fetchone()
    if not row:raise ValueError('Manual-hours record not found.')
    if row['voided_at']:raise ValueError('This manual-hours record is already voided.')
    if payroll_week_status(guild_id,row['week_start']):
        raise ValueError('This payroll week is approved/paid. Use a separately reviewed payroll correction.')
    DB.execute("""UPDATE manual_weekly_hours SET voided_at=?,voided_by=?,void_reason=?
        WHERE id=? AND guild_id=? AND voided_at IS NULL""",(now_iso(),actor_id,reason,entry_id,guild_id))
    DB.commit()
    return row


@bot.tree.command(name='addmanualhours', description='Manager: record externally tracked weekly hours (additive, not clock times)')
@app_commands.guild_only()
@app_commands.describe(employee='Employee',week_start='Sunday payroll start YYYY-MM-DD',
    work_type='Streaming or Packing',hours='Missing decimal hours, e.g. 4.3 = 4h18m',
    reason='Source of the external time record')
@app_commands.choices(work_type=[app_commands.Choice(name='Streaming',value='streaming'),
                               app_commands.Choice(name='Packing',value='packing')])
async def addmanualhours(interaction:discord.Interaction,employee:discord.Member,week_start:str,
    work_type:app_commands.Choice[str],hours:str,reason:str):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.',ephemeral=True)
    try:
        start=date.fromisoformat(week_start)
        payroll_week_bounds(start)
        if start > payroll_week_start(datetime.now(PAYROLL_TZ)):
            raise ValueError('Cannot record manual hours for a future payroll week.')
        parsed=parse_manual_weekly_hours(hours)
        async with lock:
            entry_id,wages,rate=insert_manual_weekly_hours(interaction.guild_id,employee.id,
                start.isoformat(),work_type.value,parsed,reason,interaction.user.id)
    except (ValueError,sqlite3.IntegrityError) as exc:
        return await interaction.response.send_message(f'❌ {exc}',ephemeral=True)
    await interaction.response.send_message(
        f'✅ Manual hours #{entry_id}: {employee.mention} · {start} week · {work_type.value.title()} '
        f'· **{parsed} decimal hours** × {money(rate)}/hr = **{money(wages)}**. '
        'Added to that unapproved week, separate from clocked shifts. '
        'Use `/voidmanualhours` to correct an entry.',ephemeral=True)


@bot.tree.command(name='manualhours',description='Manager: list weekly manual-hours records, including voids')
@app_commands.guild_only()
@app_commands.describe(week_start='Sunday payroll start YYYY-MM-DD',employee='Optional employee filter')
async def manualhours(interaction:discord.Interaction,week_start:str,employee:discord.Member=None):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.',ephemeral=True)
    try:payroll_week_bounds(week_start)
    except ValueError:
        return await interaction.response.send_message('Enter a Sunday week start in YYYY-MM-DD format.',ephemeral=True)
    rows=DB.execute("""SELECT * FROM manual_weekly_hours WHERE guild_id=? AND week_start=?
        AND (? IS NULL OR employee_id=?) ORDER BY id DESC""",
        (interaction.guild_id,week_start,employee.id if employee else None,employee.id if employee else None)).fetchall()
    if not rows:
        return await interaction.response.send_message('No manual weekly hours recorded for this week.',ephemeral=True)
    lines=[f'#{r["id"]} · <@{r["employee_id"]}> · {r["work_type"]} · '
           f'{r["hours_decimal"]}h · {money(r["wages_cents"])} · '
           f'{"VOID" if r["voided_at"] else "ACTIVE"} · by <@{r["created_by"]}>' for r in rows[:25]]
    await interaction.response.send_message('**Manual hours · '+week_start+'**\n'+'\n'.join(lines)
        + ('\n(Showing latest 25)' if len(rows)>25 else ''),ephemeral=True)


@bot.tree.command(name='voidmanualhours',description='Manager: void incorrect manual weekly hours before approval')
@app_commands.guild_only()
@app_commands.describe(entry_id='ID from /manualhours',reason='Reason for correction')
async def voidmanualhours(interaction:discord.Interaction,entry_id:int,reason:str):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.',ephemeral=True)
    try:
        async with lock:
            r=void_manual_weekly_hours(interaction.guild_id,entry_id,interaction.user.id,reason)
    except ValueError as exc:
        return await interaction.response.send_message(f'❌ {exc}',ephemeral=True)
    await interaction.response.send_message(f'✅ Voided manual hours #{entry_id} for <@{r["employee_id"]}>. '
        'The correction is retained in the audit history.',ephemeral=True)


def payroll_week_status(guild_id,start):
    return DB.execute('SELECT * FROM payroll_periods WHERE guild_id=? AND week_start=?',
      (guild_id,str(start))).fetchone()


def employee_payment_totals(guild_id, week_start):
    rows=DB.execute("""SELECT employee_id,SUM(amount_cents) AS total FROM employee_payments
        WHERE guild_id=? AND week_start=? AND voided_at IS NULL GROUP BY employee_id""",
        (guild_id,str(week_start))).fetchall()
    return {str(r['employee_id']): int(r['total']) for r in rows}


def payroll_display_week(guild_id, week_start, live=None):
    '''Saved approval with explicitly audited duplicate-commission corrections.

    Original snapshot_json remains immutable. Corrections are their own append-only
    ledger and may only be created by the guarded offline reconciliation utility.
    '''
    if live is None:
        live=payroll_week_data(guild_id,week_start)
    saved=payroll_week_status(guild_id,week_start)
    if saved is None:
        return live
    frozen=json.loads(saved['snapshot_json'])
    if not isinstance(frozen,dict) or not isinstance(frozen.get('employees'),dict):
        raise ValueError(f'Approved payroll snapshot for {week_start} cannot be parsed; manual audit needed.')
    try:
        adjustments=DB.execute('''SELECT employee_id,SUM(commission_cents) AS commission,
          SUM(topup_cents) AS topup FROM payroll_duplicate_commission_exclusions
          WHERE guild_id=? AND week_start=? GROUP BY employee_id''',
          (guild_id,str(week_start))).fetchall()
    except Exception as exc:
        if 'no such table: payroll_duplicate_commission_exclusions' not in str(exc): raise
        adjustments=[]
    if adjustments:
        frozen['reconciliation_adjusted']=True
        frozen['original_snapshot_gross']=int(frozen['gross'])
        for item in adjustments:
            uid=str(item['employee_id'])
            if uid not in frozen['employees']:
                raise RuntimeError('Reconciled payroll employee missing from saved snapshot')
            employee=frozen['employees'][uid]
            commission=int(item['commission']);topup=int(item['topup'])
            if employee['commission'] < commission or employee['topup'] < topup:
                raise RuntimeError(f'Reconciliation overdraws saved commission for {uid}')
            employee['commission']-=commission
            employee['topup']-=topup
            employee['gross']-=commission+topup
            employee['payable']=max(0, employee['gross']-employee.get('fine_deductions',0))
        frozen['gross']=sum(e['gross'] for e in frozen['employees'].values())
        frozen['payable']=sum(e['payable'] for e in frozen['employees'].values())
    return frozen



def approved_week_financials_match(guild_id,week_start,live=None):
    """Only tolerate snapshot fingerprint drift that leaves ALL employee totals
    and source IDs unchanged (e.g. permanently pinning legacy commissions).
    All approval blockers must also be absent. A financial discrepancy is never
    silently accepted when finalizing payment.
    """
    saved=payroll_week_status(guild_id,week_start)
    if saved is None:return False
    if live is None:live=payroll_week_data(guild_id,week_start)
    if saved['fingerprint']==live['fingerprint']:return True
    frozen=json.loads(saved['snapshot_json'])
    keys=('start','end','gross','payable','employees','pending','open','boundary')
    return not live['pending'] and not live['open'] and not live['boundary'] and all(frozen.get(k)==live.get(k) for k in keys)


def payroll_snapshot_drift(guild_id, week_start, live=None):
    saved=payroll_week_status(guild_id,week_start)
    if not saved:
        return False
    if live is None:
        live=payroll_week_data(guild_id,week_start)
    return saved['fingerprint'] != live['fingerprint']


def payroll_balances(guild_id,week_start,week=None):
    # Even callers supplying live draft calculations must not alter approved
    # balances. Payment entries stay literal external transfers only.
    week=payroll_display_week(guild_id,week_start,week)
    payments=employee_payment_totals(guild_id,week_start)
    ids=set(week['employees']) | set(payments)
    return {uid: dict(gross=week['employees'].get(uid,{}).get('gross',0),
                      fine_deductions=week['employees'].get(uid,{}).get('fine_deductions',0),
                      payable=max(0,week['employees'].get(uid,{}).get('gross',0)-week['employees'].get(uid,{}).get('fine_deductions',0)),
                      paid=payments.get(uid,0),
                      due=max(0,week['employees'].get(uid,{}).get('gross',0)-week['employees'].get(uid,{}).get('fine_deductions',0))-payments.get(uid,0))
            for uid in ids}


def record_external_payment(guild_id,employee_id,week_start,amount_cents,method,reference,paid_on,actor_id):
    """Record an actual transfer. Do not conflate this with earned wages or advances.

    Payroll may be in draft or approved state. Never edit already-paid periods.
    """
    start=date.fromisoformat(week_start)
    if start.weekday()!=6:
        raise ValueError('Pay period must begin on Sunday.')
    if amount_cents<=0:
        raise ValueError('Payment amount must be positive.')
    if method not in ('Zelle','Wire','Bank transfer','Cash','Other'):
        raise ValueError('Invalid payment method.')
    if not reference.strip():
        raise ValueError('Provide an external payment reference or confirmation.')
    date.fromisoformat(paid_on)
    with DB:
        existing=payroll_week_status(guild_id,week_start)
        if existing and existing['status']=='paid':
            raise ValueError('This pay period is already marked paid.')
        cur=DB.execute("""INSERT INTO employee_payments
          (guild_id,employee_id,week_start,amount_cents,method,reference,recorded_at,recorded_by,paid_on)
          VALUES (?,?,?,?,?,?,?,?,?)""",
          (guild_id,employee_id,week_start,amount_cents,method,reference.strip(),now_iso(),actor_id,paid_on))
        return cur.lastrowid


def void_external_payment(guild_id,payment_id,actor_id,reason):
    if not reason.strip():
        raise ValueError('A void reason is required.')
    with DB:
        row=DB.execute('SELECT * FROM employee_payments WHERE guild_id=? AND id=?',
            (guild_id,payment_id)).fetchone()
        if not row or row['voided_at']:
            raise ValueError('Payment not found or already voided.')
        period=payroll_week_status(guild_id,row['week_start'])
        if period and period['status']=='paid':
            raise ValueError('Cannot void a payment in a finalized paid week.')
        DB.execute('UPDATE employee_payments SET voided_at=?,voided_by=?,void_reason=? WHERE id=?',
                   (now_iso(),actor_id,reason.strip(),payment_id))


def payroll_week_embed(guild,week,employee_id=None):
    live=week
    current=payroll_week_status(guild.id,week['start'])
    changed=bool(current and current['fingerprint']!=live['fingerprint'])
    week=payroll_display_week(guild.id,week['start'],live)
    state=current['status'].upper() if current else 'DRAFT'
    if changed:state+=' · LIVE RECORDS DIFFER (SAVED APPROVAL SHOWN)'
    if week.get('reconciliation_adjusted'):state+=' · AUDITED LEGACY DUPLICATES EXCLUDED'
    embed=discord.Embed(title=f'💵 Weekly Payroll · {week["start"]} – {week["end"]}',
      description=f'**{state}** · Eastern Time (Sunday–Saturday)\nGross earnings and authorized deductions; verify taxes, overtime and deduction compliance separately.',
      color=discord.Color.orange() if changed else discord.Color.blurple())
    staff=week['employees']
    balances=payroll_balances(guild.id,week['start'],week)
    if employee_id is not None:
        e=staff.get(str(employee_id))
        if not e:embed.add_field(name='No earnings',value='No completed shifts or active bonuses this week.')
        else:
            embed.add_field(name=employee_name(guild,int(employee_id)),value=(
              f'Streaming: {Decimal(e["stream_hours"]):.2f} h · Packing: {Decimal(e["packing_hours"]):.2f} h\n'
              f'Total hours: {Decimal(e["hours"]):.2f} h\n'
              f'Manual: {Decimal(e.get("manual_hours","0")):.2f} h · {money(int(e.get("manual_base",0)))} wages\n'
              f'Base: {money(e["base"])}\nCommission: {money(e["commission"])}\n'
              f'Character Break top-ups: {money(e["topup"])}\nBonus: {money(e["bonus"])}\n'
              f'**Estimated gross: {money(e["gross"])}**\n'
              f'Authorized fines (deductions): −{money(e["fine_deductions"])}\n'
              f'**Payable after fines: {money(e["payable"])}**\n'
              f'Paid already: {money(balances[str(employee_id)]["paid"])}\n'
              f'**Balance due: {money(balances[str(employee_id)]["due"])}**'),inline=False)
    else:
        embed.add_field(name='Total estimated gross',value=f'**{money(week["gross"])}**',inline=False)
        embed.add_field(name='Authorized fines / Payable after fines',
          value=f'−{money(week["gross"]-week["payable"])} / **{money(week["payable"])}**',inline=False)
        lines=[f'{employee_name(guild,int(uid))}: {money(e["gross"])} gross · {money(balances[uid]["due"])} due'
          for uid,e in sorted(staff.items(),key=lambda kv:-kv[1]['gross'])]
        if lines:embed.add_field(name='Employee totals',value='\n'.join(lines[:18])[:1024],inline=False)
        else:embed.add_field(name='No entries',value='No completed shifts or bonuses.',inline=False)
        embed.add_field(name='Already sent / Balance due',value=(
            f'{money(sum(v["paid"] for v in balances.values()))} paid · '
            f'**{money(sum(v["due"] for v in balances.values()))} remaining**'),inline=False)
    issues=[]
    if week['pending']:issues.append(f'{len(week["pending"])} pending commission(s)')
    if week['open']:issues.append(f'{len(week["open"])} open shift(s) that might affect this week')
    if week['boundary']:issues.append(f'{len(week["boundary"])} cross-week shift(s) requiring manual review')
    if changed:issues.append('Live payroll differs from saved approval. Saved amounts shown; DO NOT pay differences without reconciliation')
    if issues:embed.add_field(name='⚠️ Review required',value='\n'.join(issues),inline=False)
    if current:
        embed.add_field(name='Approval record',value=f'Approved {current["approved_at"]} by {employee_name(guild,current["approved_by"])}',inline=False)
        if current['status']=='paid':embed.add_field(name='Marked paid',value=f'{current["paid_at"]} · Ref: {discord.utils.escape_markdown(current["payment_reference"] or "n/a")}',inline=False)
    embed.set_footer(text='Approved and paid weeks show saved approval values. Due = saved payable minus RECORDED transfers (may omit externally paid funds). Verify external statements.')
    return embed


# V24: manager-only day-by-day payroll allocation.
# Daily totals are an explanation of the SAME v23 weekly calculation, not a
# replacement ledger. Carryovers display on the opening Sunday of their
# payable week so totals cannot silently differ from /weeklypayroll.

def daily_payroll_data(guild_id, week_start):
    if isinstance(week_start, str):
        week_start = date.fromisoformat(week_start)
    week = payroll_week_data(guild_id, week_start.isoformat())
    low, high = payroll_week_bounds(week_start)
    days = [(week_start + timedelta(days=i)).isoformat() for i in range(7)]
    records = {d: {} for d in days}

    def get(d, uid):
        return records[d].setdefault(str(uid), dict(stream_hours=Decimal(0),
            packing_hours=Decimal(0), base=0, commission=0, topup=0,
            bonus=0, gross=0, commission_ids=[], bonus_ids=[]))

    shifts = DB.execute('''SELECT id,user_id,clock_in,clock_out,hourly_rate_cents,work_type
        FROM shifts WHERE guild_id=? AND clock_out IS NOT NULL
        AND clock_in<? AND clock_out>? ORDER BY id''',
        (guild_id,high.isoformat(),low.isoformat())).fetchall()
    for r in shifts:
        original_start = datetime.fromisoformat(r['clock_in'])
        original_end = datetime.fromisoformat(r['clock_out'])
        begin = max(original_start, low)
        end = min(original_end, high)
        if begin >= end:
            continue
        rate = int(r['hourly_rate_cents'] if r['hourly_rate_cents'] is not None else HOURLY_RATE_CENTS)
        # Match weekly rounding exactly: give any penny remainder to the last
        # worked day in this payroll week.
        full_hours = Decimal(str((end-begin).total_seconds())) / Decimal(3600)
        expected_base = int((full_hours * rate).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
        pieces = []
        cursor = begin
        while cursor < end:
            local_day = cursor.astimezone(PAYROLL_TZ).date()
            tomorrow = datetime.combine(local_day+timedelta(days=1),time.min,tzinfo=PAYROLL_TZ).astimezone(timezone.utc)
            piece_end = min(end,tomorrow)
            if piece_end <= cursor:
                raise RuntimeError('Invalid local midnight while calculating daily pay')
            pieces.append((local_day.isoformat(), Decimal(str((piece_end-cursor).total_seconds()))/Decimal(3600)))
            cursor = piece_end
        allocated = 0
        for i,(d,hours) in enumerate(pieces):
            cents = expected_base-allocated if i == len(pieces)-1 else int((hours*rate).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
            allocated += cents
            item=get(d,r['user_id'])
            if r['work_type']=='packing':
                item['packing_hours'] += hours
            else:
                item['stream_hours'] += hours
            item['base'] += cents

    rows = DB.execute('''SELECT c.*,s.clock_out FROM commissions c
        JOIN shifts s ON s.id=c.shift_id WHERE c.guild_id=? ORDER BY c.id''',(guild_id,)).fetchall()
    pending = []
    carried = []
    for r in rows:
        target_week = assigned_commission_week(guild_id,r)
        if target_week != week_start.isoformat():
            continue
        finish = r['finished_at'] or r['clock_out'] or r['submitted_at']
        actual_date = datetime.fromisoformat(finish).astimezone(PAYROLL_TZ).date()
        original_week = payroll_week_start(actual_date).isoformat()
        d = actual_date.isoformat() if original_week == target_week else week_start.isoformat()
        if r['status']=='pending':
            pending.append(dict(id=r['id'],employee_id=r['user_id'],finished=actual_date.isoformat(),display_date=d))
            continue
        if r['status']!='approved':
            continue
        item=get(d,r['user_id'])
        item['commission']+=int(r['commission_cents'])
        item['topup']+=int(r['guarantee_topup_cents'] or 0)
        item['commission_ids'].append(r['id'])
        if original_week != target_week:
            carried.append(dict(id=r['id'],employee_id=r['user_id'],finished=actual_date.isoformat(),display_date=d))

    bonuses = DB.execute('''SELECT id,employee_id,amount_cents,created_at FROM bonuses
        WHERE guild_id=? AND created_at>=? AND created_at<? AND voided_at IS NULL ORDER BY id''',
        (guild_id,low.isoformat(),high.isoformat())).fetchall()
    for r in bonuses:
        d=datetime.fromisoformat(r['created_at']).astimezone(PAYROLL_TZ).date().isoformat()
        item=get(d,r['employee_id'])
        item['bonus']+=int(r['amount_cents'])
        item['bonus_ids'].append(r['id'])

    # No daily timestamps exist for these externally tracked weekly totals.
    # Display in Sunday's reconciliation bucket ONLY; Sunday is not a claimed work date.
    for r in manual_weekly_hours_rows(guild_id,week_start.isoformat()):
        item=get(week_start.isoformat(),r['employee_id'])
        duration=Decimal(r['hours_decimal'])
        hour_key='stream_hours' if r['work_type']=='streaming' else 'packing_hours'
        item[hour_key]+=duration
        item['base']+=int(r['wages_cents'])

    for day_records in records.values():
        for item in day_records.values():
            item['gross']=item['base']+item['commission']+item['topup']+item['bonus']

    week_daily_gross=sum(x['gross'] for day in records.values() for x in day.values())
    if week_daily_gross != week['gross']:
        raise RuntimeError(f'Daily pay reconciliation failed ({week_daily_gross} != {week["gross"]})')
    for uid, e in week['employees'].items():
        if sum(day.get(uid,{}).get('gross',0) for day in records.values()) != e['gross']:
            raise RuntimeError(f'Employee daily pay reconciliation failed: {uid}')
    return dict(week=week, days=records, pending=pending, carried=carried)


def daily_payroll_embed(guild,data,selected=None,day=None):
    week=data['week']
    title=f'📅 Daily Pay · {week["start"]} – {week["end"]}'
    drift=payroll_snapshot_drift(guild.id,week['start'])
    warning=('\n⚠️ LIVE DAILY ESTIMATE DIFFERS FROM SAVED APPROVAL. '
             'Do not use this daily breakdown to pay a finalized week.\n') if drift else ''
    embed=discord.Embed(title=title,description=(
        warning + 'Eastern Time · Sunday–Saturday · **estimated gross only**\n'
        'Base wages split at local midnight. Commissions appear on their finishing day; '
        'approved carryovers appear on the next payable Sunday. '
        '**Weekly manual hours appear in Sunday’s reconciliation bucket, not as claimed Sunday shifts.**'),color=discord.Color.blurple())
    def total_for(d, key):
        entries=data['days'][d]
        if selected is not None:
            item=entries.get(str(selected))
            return (item or {}).get(key,0)
        return sum(item[key] for item in entries.values())
    if selected is not None:
        embed.add_field(name='Employee',value=employee_name(guild,int(selected)),inline=False)
    if day is None:
        lines=[]
        for d in data['days']:
            day_label=datetime.strptime(d,'%Y-%m-%d').strftime('%a %b %d')
            hours=sum((total_for(d,k) for k in ('stream_hours','packing_hours')),Decimal(0))
            lines.append(f'**{day_label}** · {hours:.2f} h · **{money(total_for(d,"gross"))}**')
        embed.add_field(name='Seven-day earnings',value='\n'.join(lines),inline=False)
        ds=list(data['days'])
    else:
        ds=[day]
        if selected is None:
            lines=[f'{employee_name(guild,int(uid))}: **{money(row["gross"])}**'
                   for uid,row in sorted(data['days'][day].items(),key=lambda pair:-pair[1]['gross'])]
            embed.add_field(name='Employees on this day',value='\n'.join(lines[:20])[:1024] or 'No earnings.',inline=False)
        else:
            r=data['days'][day].get(str(selected),{})
            embed.add_field(name=f'{day} breakdown',value=(
                f'Streaming: {r.get("stream_hours",Decimal(0)):.2f} hours\n'
                f'Packing: {r.get("packing_hours",Decimal(0)):.2f} hours\n'
                f'Base: {money(r.get("base",0))}\n'
                f'Approved commission: {money(r.get("commission",0))}\n'
                f'Character Break top-up: {money(r.get("topup",0))}\n'
                f'Bonuses: {money(r.get("bonus",0))}\n'
                f'**Total: {money(r.get("gross",0))}**'),inline=False)
    gross=sum(total_for(d,'gross') for d in ds)
    embed.add_field(name='Displayed total',value=f'**{money(gross)}**',inline=False)
    if data['pending']:
        embed.add_field(name='Pending commissions',value=f'{len(data["pending"])} awaiting approval (not included in pay).',inline=False)
    if data['carried']:
        embed.add_field(name='Prior-period carryovers',value=f'{len(data["carried"])} approved commission(s) carried into this week and shown on Sunday.',inline=False)
    if week['open']:
        embed.add_field(name='Open shifts',value=f'{len(week["open"])} open shift(s) may change these estimates.',inline=False)
    embed.set_footer(text='Use Weekly Payroll to review/approve/mark paid. Overtime, taxes and deductions are excluded.')
    return embed


class DailyPayrollView(discord.ui.View):
    def __init__(self,owner,guild,week_start,selected=None,day=None):
        super().__init__(timeout=600)
        self.owner=owner;self.guild_id=guild.id;self.week_start=week_start
        self.selected=selected;self.day=day
        today=datetime.now(PAYROLL_TZ).date()
        sunday=payroll_week_start(today)
        options=[]
        for i in range(-2,12):
            d=sunday-timedelta(weeks=i)
            options.append(discord.SelectOption(label=f'{d:%b %d} – {(d+timedelta(days=6)):%b %d, %Y}',value=d.isoformat(),default=d.isoformat()==week_start))
        week_select=discord.ui.Select(placeholder='Pay period',options=options,row=0)
        async def select_week(interaction):
            self.week_start=week_select.values[0];self.selected=None;self.day=None
            await self.show(interaction)
        week_select.callback=select_week;self.add_item(week_select)
        data=daily_payroll_data(guild.id,week_start)
        ids=set(data['week']['employees'])
        ids.update(str(x['employee_id']) for x in data['pending'])
        staff_options=[discord.SelectOption(label='All employees',value='all',default=selected is None)]
        for uid in sorted(ids,key=lambda u: employee_name(guild,int(u)).lower())[:24]:
            staff_options.append(discord.SelectOption(label=employee_name(guild,int(uid))[:100],value=uid,default=str(selected)==uid))
        staff_select=discord.ui.Select(placeholder='Employee',options=staff_options,row=1)
        async def select_employee(interaction):
            self.selected=None if staff_select.values[0]=='all' else int(staff_select.values[0])
            await self.show(interaction)
        staff_select.callback=select_employee;self.add_item(staff_select)
        day_options=[discord.SelectOption(label='All 7 days',value='all',default=day is None)]
        for d in data['days']:
            label=datetime.strptime(d,'%Y-%m-%d').strftime('%a, %b %d')
            day_options.append(discord.SelectOption(label=label,value=d,default=day==d))
        day_select=discord.ui.Select(placeholder='Day',options=day_options,row=2)
        async def select_day(interaction):
            self.day=None if day_select.values[0]=='all' else day_select.values[0]
            await self.show(interaction)
        day_select.callback=select_day;self.add_item(day_select)
        export=discord.ui.Button(label='Download Daily CSV',emoji='📄',style=discord.ButtonStyle.secondary,row=3)
        async def export_click(interaction):
            await interaction.response.defer(ephemeral=True)
            d=daily_payroll_data(interaction.guild_id,self.week_start)
            output=io.StringIO();writer=csv.writer(output)
            writer.writerow(['pay_period_start','work_or_pay_date_et','discord_user_id','employee_name','streaming_hours','packing_hours','base_pay','approved_commission','character_break_topup','bonuses','estimated_gross'])
            for day,people in d['days'].items():
                for uid,e in sorted(people.items()):
                    writer.writerow([self.week_start,day,uid,employee_name(interaction.guild,int(uid)),
                        f'{e["stream_hours"]:.4f}',f'{e["packing_hours"]:.4f}',money(e['base']),
                        money(e['commission']),money(e['topup']),money(e['bonus']),money(e['gross'])])
            await interaction.followup.send('Manager-only seven-day CSV for all employees. Carryovers are shown on their payable Sunday.',file=discord.File(io.BytesIO(output.getvalue().encode('utf-8-sig')),filename=f'etbeast_demo_daily_pay_{self.week_start}.csv'),ephemeral=True)
        export.callback=export_click;self.add_item(export)

    async def interaction_check(self,interaction):
        if interaction.user.id!=self.owner or interaction.guild_id!=self.guild_id or not allowed_guild(interaction) or not manager(interaction):
            await interaction.response.send_message('Manager-only. Open your own /dashboard.',ephemeral=True)
            return False
        return True

    async def show(self,interaction):
        await interaction.response.defer()
        data=daily_payroll_data(self.guild_id,self.week_start)
        ids=[int(x) for x in data['week']['employees']]
        await preload_employee_names(interaction.guild,ids)
        view=DailyPayrollView(self.owner,interaction.guild,self.week_start,self.selected,self.day)
        await interaction.edit_original_response(embed=daily_payroll_embed(interaction.guild,data,self.selected,self.day),view=view)


@bot.tree.command(name='dailypay',description='Manager: view daily employee earnings and a seven-day payroll breakdown')
@app_commands.guild_only()
async def dailypay(interaction:discord.Interaction):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.',ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    start=payroll_week_start(datetime.now(PAYROLL_TZ)).isoformat()
    data=daily_payroll_data(interaction.guild_id,start)
    await preload_employee_names(interaction.guild,[int(x) for x in data['week']['employees']])
    await interaction.followup.send(embed=daily_payroll_embed(interaction.guild,data),
        view=DailyPayrollView(interaction.user.id,interaction.guild,start),ephemeral=True)


# V25: private payment ledger controls. Only confirmed external payments belong here.
class PaymentRecordModal(discord.ui.Modal,title='Record payment already sent'):
    amount=discord.ui.TextInput(label='Amount sent (USD)',placeholder='1000.00',max_length=20)
    method=discord.ui.TextInput(label='Payment method (Zelle/Wire/etc.)',default='Zelle',max_length=25)
    reference=discord.ui.TextInput(label='Confirmation / transfer reference',placeholder='Bank confirmation ID, or manually recorded reference',max_length=150)
    paid_on=discord.ui.TextInput(label='Actual date sent (YYYY-MM-DD)',placeholder='2030-01-12',max_length=10)
    def __init__(self,owner,guild_id,week_start,employee_id):
        super().__init__();self.owner=owner;self.guild_id=guild_id;self.week_start=week_start;self.employee_id=employee_id
        self.paid_on.default=datetime.now(PAYROLL_TZ).date().isoformat()
    async def on_submit(self,interaction):
        if interaction.user.id!=self.owner or not manager(interaction) or interaction.guild_id!=self.guild_id:
            return await interaction.response.send_message('Manager-only.',ephemeral=True)
        try:
            value=Decimal(str(self.amount.value).strip().replace('$','').replace(',',''))
            if not value.is_finite() or value<=0 or value.as_tuple().exponent < -2:
                raise ValueError('Enter a positive amount with no more than two decimal places.')
            cents=int(value*100)
            pid=record_external_payment(self.guild_id,self.employee_id,self.week_start,cents,
                str(self.method.value).strip(),str(self.reference.value),str(self.paid_on.value).strip(),interaction.user.id)
        except (ValueError,InvalidOperation) as exc:
            return await interaction.response.send_message(f'Could not record payment: {exc}',ephemeral=True)
        week=payroll_week_data(self.guild_id,self.week_start)
        due=payroll_balances(self.guild_id,self.week_start,week)[str(self.employee_id)]['due']
        await interaction.response.send_message(
            f'✅ Payment #{pid} recorded for {employee_name(interaction.guild,self.employee_id)}: '
            f'{money(cents)} sent on {self.paid_on.value}.\n'
            f'**Balance due for {self.week_start}: {money(due)}**. '
            'No money was transferred by ETBeast.',ephemeral=True)


class PaymentVoidModal(discord.ui.Modal,title='Void recorded payment'):
    reason=discord.ui.TextInput(label='Why is this payment entry wrong?',max_length=250)
    def __init__(self,owner,guild_id,payment_id):
        super().__init__();self.owner=owner;self.guild_id=guild_id;self.payment_id=payment_id
    async def on_submit(self,interaction):
        if interaction.user.id!=self.owner or not manager(interaction) or interaction.guild_id!=self.guild_id:
            return await interaction.response.send_message('Manager-only.',ephemeral=True)
        try: void_external_payment(self.guild_id,self.payment_id,interaction.user.id,str(self.reason.value))
        except ValueError as exc: return await interaction.response.send_message(str(exc),ephemeral=True)
        await interaction.response.send_message(f'✅ Payment #{self.payment_id} voided in the ledger. External transfers are not reversed.',ephemeral=True)


class PaymentLedgerView(discord.ui.View):
    def __init__(self,owner,guild,week_start,employee_id=None):
        super().__init__(timeout=600)
        self.owner=owner;self.guild_id=guild.id;self.week_start=week_start;self.employee_id=employee_id
        today=payroll_week_start(datetime.now(PAYROLL_TZ))
        options=[]
        for i in range(-2,12):
            d=today-timedelta(weeks=i)
            options.append(discord.SelectOption(label=f'{d:%b %d} – {(d+timedelta(days=6)):%b %d}',value=d.isoformat(),default=d.isoformat()==week_start))
        week_select=discord.ui.Select(placeholder='Pay period',options=options,row=0)
        async def choose_week(interaction):
            self.week_start=week_select.values[0];self.employee_id=None
            await self.show(interaction)
        week_select.callback=choose_week;self.add_item(week_select)
        week=payroll_week_data(guild.id,week_start)
        ids=set(week['employees']) | set(employee_payment_totals(guild.id,week_start))
        # Allow recording payments even before any wages have been clocked this week.
        ids.update(str(r['user_id']) for r in DB.execute('SELECT DISTINCT user_id FROM shifts WHERE guild_id=?',(guild.id,)).fetchall())
        members=[discord.SelectOption(label='All employees',value='all',default=employee_id is None)]
        for uid in sorted(ids,key=lambda x:employee_name(guild,int(x)).lower())[:24]:
            members.append(discord.SelectOption(label=employee_name(guild,int(uid))[:100],value=uid,default=str(employee_id)==uid))
        pick=discord.ui.Select(placeholder='Employee (choose before recording)',options=members,row=1)
        async def choose_employee(interaction):
            self.employee_id=None if pick.values[0]=='all' else int(pick.values[0])
            await self.show(interaction)
        pick.callback=choose_employee;self.add_item(pick)
        status=payroll_week_status(guild.id,week_start)
        record=discord.ui.Button(label='Record Payment',emoji='💸',style=discord.ButtonStyle.success,row=2,
            disabled=employee_id is None or bool(status and status['status']=='paid'))
        async def record_click(interaction):
            await interaction.response.send_modal(PaymentRecordModal(self.owner,self.guild_id,self.week_start,self.employee_id))
        record.callback=record_click;self.add_item(record)
        rows=DB.execute("""SELECT id,amount_cents,employee_id FROM employee_payments WHERE guild_id=? AND week_start=? AND voided_at IS NULL
            AND (? IS NULL OR employee_id=?) ORDER BY id DESC LIMIT 25""",
            (guild.id,week_start,employee_id,employee_id)).fetchall()
        if rows and not (status and status['status']=='paid'):
            void_select=discord.ui.Select(placeholder='Void an incorrect payment',row=3,
                options=[discord.SelectOption(label=f'#{r["id"]} · {employee_name(guild,r["employee_id"])[:45]} · {money(r["amount_cents"])}',value=str(r['id'])) for r in rows])
            async def void_click(interaction):
                await interaction.response.send_modal(PaymentVoidModal(self.owner,self.guild_id,int(void_select.values[0])))
            void_select.callback=void_click;self.add_item(void_select)
        export=discord.ui.Button(label='Payment Ledger CSV',emoji='📄',style=discord.ButtonStyle.secondary,row=4)
        async def export_click(interaction):
            rows=DB.execute('SELECT * FROM employee_payments WHERE guild_id=? AND week_start=? ORDER BY id',
                (interaction.guild_id,self.week_start)).fetchall()
            f=io.StringIO();w=csv.writer(f)
            w.writerow(['payment_id','pay_period_sunday','employee_id','employee','amount_sent','method','payment_date','reference','recorded_at','recorded_by','voided_at','voided_by','void_reason'])
            for r in rows:
                w.writerow([r['id'],r['week_start'],r['employee_id'],employee_name(interaction.guild,r['employee_id']),
                    money(r['amount_cents']),r['method'],r['paid_on'],r['reference'],r['recorded_at'],r['recorded_by'],r['voided_at'],r['voided_by'],r['void_reason']])
            await interaction.response.send_message('External payment ledger export.',file=discord.File(io.BytesIO(f.getvalue().encode('utf-8-sig')),filename=f'payments_{self.week_start}.csv'),ephemeral=True)
        export.callback=export_click;self.add_item(export)

    async def interaction_check(self,interaction):
        if interaction.user.id!=self.owner or not manager(interaction) or interaction.guild_id!=self.guild_id or not allowed_guild(interaction):
            await interaction.response.send_message('Manager-only: open your own /dashboard.',ephemeral=True)
            return False
        return True

    async def show(self,interaction):
        await interaction.response.defer()
        await interaction.edit_original_response(embed=payment_ledger_embed(interaction.guild,self.week_start,self.employee_id),
            view=PaymentLedgerView(self.owner,interaction.guild,self.week_start,self.employee_id))


def payment_ledger_embed(guild,week_start,employee_id=None):
    live=payroll_week_data(guild.id,week_start)
    week=payroll_display_week(guild.id,week_start,live)
    status=payroll_week_status(guild.id,week_start)
    balances=payroll_balances(guild.id,week_start,week)
    chosen=balances if employee_id is None else {str(employee_id):balances.get(str(employee_id),{'gross':0,'paid':0,'due':0})}
    embed=discord.Embed(title=f'💸 Payments & Balances · {week_start}',description=(
        f'{status["status"].upper() if status else "DRAFT"} · Payments made outside Discord only.\n'
        'An early payment reduces balance due, **not earnings**.'),color=discord.Color.blurple())
    embed.add_field(name='Total gross earned',value=money(sum(v['gross'] for v in chosen.values())))
    embed.add_field(name='Approved fine deductions',value='−'+money(sum(v.get('fine_deductions',0) for v in chosen.values())))
    embed.add_field(name='Already sent',value=money(sum(v['paid'] for v in chosen.values())))
    embed.add_field(name='Balance due',value=f'**{money(sum(v["due"] for v in chosen.values()))}**')
    if employee_id is None:
        lines=[f'{employee_name(guild,int(uid))}: {money(v["paid"])} sent / **{money(v["due"])} due**' for uid,v in sorted(chosen.items()) if v['gross'] or v['paid']]
        embed.add_field(name='Employee balances',value='\n'.join(lines[:18])[:1024] or 'No earnings or payments yet.',inline=False)
    else:
        rows=DB.execute("""SELECT * FROM employee_payments WHERE guild_id=? AND week_start=? AND employee_id=? ORDER BY id DESC LIMIT 12""",
            (guild.id,week_start,employee_id)).fetchall()
        lines=[f'#{r["id"]} · {money(r["amount_cents"])} · {r["method"]} · {r["paid_on"]}' + (' · VOID' if r['voided_at'] else '') for r in rows]
        embed.add_field(name='Payment history',value='\n'.join(lines)[:1024] or 'No payments recorded.',inline=False)
    embed.set_footer(text='Approved figures are frozen; balance due reflects only transfers recorded here, not proof of an unpaid external transfer. Verify bank/provider before payment.')
    return embed


@bot.tree.command(name='payments',description='Manager: record early transfers and see unpaid payroll balances')
@app_commands.guild_only()
async def payments(interaction:discord.Interaction):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.',ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    week_start=payroll_week_start(datetime.now(PAYROLL_TZ)).isoformat()
    await interaction.followup.send(embed=payment_ledger_embed(interaction.guild,week_start),
        view=PaymentLedgerView(interaction.user.id,interaction.guild,week_start),ephemeral=True)


# V32: Exact pay-period finalization checklist; no payroll mutation.
def payroll_checklist(guild_id, week_start):
    live = payroll_week_data(guild_id, week_start)
    week = payroll_display_week(guild_id, week_start, live)
    status = payroll_week_status(guild_id, week_start)
    balances = payroll_balances(guild_id, week_start, week)
    problems = []
    if week['pending']:
        problems.append(f'{len(week["pending"])} commission(s) pending review')
    if week['open']:
        problems.append(f'{len(week["open"])} shift(s) still open')
    if week['boundary']:
        problems.append(f'{len(week["boundary"])} shift(s) require boundary review')
    if status and status['fingerprint'] != live['fingerprint']:
        problems.append('Live records differ from saved approval. Historical approval is locked; use offline reconciliation, not reapproval')
    if not status:
        problems.append('Week not yet approved')
    elif status['status'] == 'approved':
        outstanding = sum(v['due'] for v in balances.values())
        if outstanding:
            problems.append(f'{money(outstanding)} not matched by recorded payments; verify external transfers before assuming actually due')
    if datetime.now(timezone.utc) < payroll_week_bounds(week_start)[1]:
        problems.append('Pay period has not ended')
    return week, status, balances, problems


def payroll_checklist_embed(guild_id, week_start):
    week, status, balances, problems = payroll_checklist(guild_id, week_start)
    embed = discord.Embed(title=f'✅ Payroll Finalization Checklist · {week["start"]} – {week["end"]}',
                          description=('⚠️ **Needs attention**' if problems else '✅ **No outstanding checks detected**') +
                          '\nBased on the exact Sunday–Saturday payroll period.', color=discord.Color.orange() if problems else discord.Color.green())
    embed.add_field(name='Status',value=status['status'].upper() if status else 'DRAFT')
    embed.add_field(name='Gross earnings',value=money(week['gross']))
    embed.add_field(name='Authorized fines / Payable',value=f'−{money(week["gross"]-week["payable"])} / {money(week["payable"])}')
    embed.add_field(name='Paid externally / Remaining',value=f'{money(sum(v["paid"] for v in balances.values()))} / {money(sum(v["due"] for v in balances.values()))}',inline=False)
    embed.add_field(name='Checklist',value='\n'.join(f'• {x}' for x in problems)[:1024] if problems else 'All automated checks clear.',inline=False)
    embed.set_footer(text='An automated readiness check, not proof money was transferred or employees were fully paid.')
    return embed


@bot.tree.command(name='paychecklist',description='Manager: review payroll blockers for an exact Sunday-start week')
@app_commands.guild_only()
async def paychecklist_command(interaction:discord.Interaction, week_start:str=''):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager permission required.',ephemeral=True)
    try:
        start=week_start or payroll_week_start(datetime.now(PAYROLL_TZ)).isoformat()
        payroll_week_bounds(start)
    except ValueError:
        return await interaction.response.send_message('Use a Sunday date in YYYY-MM-DD format.',ephemeral=True)
    await interaction.response.send_message(embed=payroll_checklist_embed(interaction.guild_id,start),ephemeral=True)


class WeeklyPayrollView(discord.ui.View):
    def __init__(self,owner,guild,week_start,selected=None):
        super().__init__(timeout=600)
        self.owner=owner;self.guild_id=guild.id;self.week_start=week_start;self.selected=selected
        today=datetime.now(PAYROLL_TZ).date()
        sunday=payroll_week_start(today)
        options=[]
        for i in range(-2,12):
            d=sunday-timedelta(weeks=i)
            options.append(discord.SelectOption(label=f'{d:%b %d} – {(d+timedelta(days=6)):%b %d, %Y}',value=d.isoformat(),default=d.isoformat()==week_start))
        chooser=discord.ui.Select(placeholder='Choose exact payroll week',options=options,row=0)
        async def change_week(interaction):
            self.week_start=chooser.values[0];self.selected=None
            await self.show(interaction)
        chooser.callback=change_week;self.add_item(chooser)
        live_week=payroll_week_data(guild.id,week_start)
        week=payroll_display_week(guild.id,week_start,live_week)
        emps=[discord.SelectOption(label='All employees',value='all',default=selected is None)]
        for uid in list(week['employees'])[:24]:
            emps.append(discord.SelectOption(label=employee_name(guild,int(uid))[:100],value=uid,default=uid==str(selected)))
        pick=discord.ui.Select(placeholder='Choose employee',options=emps,row=1)
        async def choose_employee(interaction):
            self.selected=None if pick.values[0]=='all' else int(pick.values[0])
            await self.show(interaction)
        pick.callback=choose_employee;self.add_item(pick)
        saved=payroll_week_status(guild.id,week_start)
        changed=bool(saved and saved['fingerprint']!=live_week['fingerprint'])
        approve=discord.ui.Button(label='Approve Week',emoji='✅',style=discord.ButtonStyle.success,row=2,
          disabled=bool(saved) or bool(week['pending'] or week['open'] or week['boundary']) or datetime.now(timezone.utc)<payroll_week_bounds(week_start)[1])
        async def approve_click(interaction):
            await interaction.response.send_message('Approve this reviewed week? This freezes an auditable snapshot of the *estimated gross* earnings. Confirm overtime, taxes, and other payroll adjustments outside the bot.',
              view=WeeklyPayrollConfirm(self.owner,self.week_start,'approve'),ephemeral=True)
        approve.callback=approve_click;self.add_item(approve)
        paid=discord.ui.Button(label='Mark Paid',emoji='💸',style=discord.ButtonStyle.primary,row=2,
          disabled=not saved or saved['status']!='approved' or not approved_week_financials_match(guild.id,week_start,live_week))
        async def paid_click(interaction):
            await interaction.response.send_modal(WeeklyPaidModal(self.owner,self.week_start))
        paid.callback=paid_click;self.add_item(paid)
        # V51: never replace an approved snapshot via a button. Some approved
        # historical weeks were already paid externally even if ETBeast wasn't
        # marked paid. An offline, evidence-backed reconciliation is required.
        checklist=discord.ui.Button(label='Finalization Checklist',emoji='✅',style=discord.ButtonStyle.secondary,row=4)
        async def checklist_click(interaction):
            await interaction.response.send_message(embed=payroll_checklist_embed(interaction.guild_id,self.week_start),ephemeral=True)
        checklist.callback=checklist_click;self.add_item(checklist)
        ledger=discord.ui.Button(label='Payments / Balances',emoji='💰',style=discord.ButtonStyle.secondary,row=3)
        async def ledger_click(interaction):
            await interaction.response.send_message(embed=payment_ledger_embed(interaction.guild,self.week_start),
                view=PaymentLedgerView(self.owner,interaction.guild,self.week_start),ephemeral=True)
        ledger.callback=ledger_click;self.add_item(ledger)
        export=discord.ui.Button(label='Week CSV',emoji='📄',style=discord.ButtonStyle.secondary,row=2)
        async def export_click(interaction):
            live=payroll_week_data(interaction.guild_id,self.week_start)
            w=payroll_display_week(interaction.guild_id,self.week_start,live)
            saved=payroll_week_status(interaction.guild_id,self.week_start)
            out=io.StringIO();cw=csv.writer(out)
            cw.writerow(['week_start','week_end','status','employee_discord_id','employee_nickname','streaming_hours','packing_hours','total_hours','manual_hours','manual_base_wages','base_wages','approved_commissions','character_break_topups','bonuses','reconciled_gross','authorized_fines','payable_after_fines','payments_sent','remaining_balance'])
            for uid,e in sorted(w['employees'].items()):
                cw.writerow([w['start'],w['end'],saved['status'] if saved else 'draft',uid,employee_name(interaction.guild,int(uid)),
                  e['stream_hours'],e['packing_hours'],e['hours'],e.get('manual_hours','0'),money(int(e.get('manual_base',0))),money(e['base']),money(e['commission']),money(e['topup']),money(e['bonus']),money(e['gross']),money(e['fine_deductions']),money(e['payable']),money(employee_payment_totals(interaction.guild_id,w['start']).get(uid,0)),money(e['payable']-employee_payment_totals(interaction.guild_id,w['start']).get(uid,0))])
            await interaction.response.send_message('Exact weekly earnings export (not final payroll).',file=discord.File(io.BytesIO(out.getvalue().encode('utf-8-sig')),filename=f'etbeast_demo_week_{w["start"]}.csv'),ephemeral=True)
        export.callback=export_click;self.add_item(export)

    async def interaction_check(self,interaction):
        if interaction.user.id!=self.owner or interaction.guild_id!=self.guild_id or not allowed_guild(interaction) or not manager(interaction):
            await interaction.response.send_message('Manager-only. Open your own /dashboard.',ephemeral=True)
            return False
        return True

    async def show(self,interaction):
        week=payroll_week_data(self.guild_id,self.week_start)
        await preload_employee_names(interaction.guild,[int(x) for x in week['employees']])
        view=WeeklyPayrollView(self.owner,interaction.guild,self.week_start,self.selected)
        await interaction.response.edit_message(embed=payroll_week_embed(interaction.guild,week,self.selected),view=view)


class WeeklyPayrollConfirm(discord.ui.View):
    def __init__(self,owner,week_start,action):
        super().__init__(timeout=120);self.owner=owner;self.week_start=week_start;self.action=action

    @discord.ui.button(label='Confirm Reviewed Amount',style=discord.ButtonStyle.success)
    async def confirm(self,interaction,button):
        if interaction.user.id!=self.owner or not manager(interaction) or not allowed_guild(interaction):
            return await interaction.response.send_message('Manager-only.',ephemeral=True)
        week=payroll_week_data(interaction.guild_id,self.week_start)
        if datetime.now(timezone.utc)<payroll_week_bounds(self.week_start)[1]:
            return await interaction.response.send_message('Cannot approve a week before it ends.',ephemeral=True)
        if week['pending'] or week['open'] or week['boundary']:
            return await interaction.response.send_message('Cannot approve: resolve pending commissions, open shifts and cross-week shifts first.',ephemeral=True)
        saved=payroll_week_status(interaction.guild_id,self.week_start)
        if self.action=='approve' and saved:
            return await interaction.response.send_message('Week was already approved or marked paid. No duplicate approvals.',ephemeral=True)
        if self.action=='reapprove':
            return await interaction.response.send_message(
                'Historical reapproval is locked for payroll safety. Use saved snapshots and a documented external reconciliation.',ephemeral=True)
        when=now_iso()
        with DB:
            DB.execute('''INSERT INTO payroll_periods
              (guild_id,week_start,status,gross_cents,fingerprint,snapshot_json,approved_at,approved_by)
              VALUES (?,?,?,?,?,?,?,?)''',
              (interaction.guild_id,week['start'],'approved',week['gross'],week['fingerprint'],json.dumps(week,sort_keys=True),when,interaction.user.id))
            DB.execute('INSERT INTO payroll_period_audit(guild_id,week_start,actor_id,action,occurred_at,details) VALUES(?,?,?,?,?,?)',
              (interaction.guild_id,week['start'],interaction.user.id,self.action,when,
               f'New gross {money(week["gross"])}; prior gross {money(saved["gross_cents"])}' if saved else f'Estimated gross {money(week["gross"])}'))
        await interaction.response.send_message('✅ Week approval snapshot saved. **This does not pay employees.** Verify final payroll through your provider, then use Mark Paid.',ephemeral=True)


class WeeklyPaidModal(discord.ui.Modal,title='Record completed payroll payment'):
    reference=discord.ui.TextInput(label='External payroll run/payment reference',placeholder='e.g. provider batch ID 12345',min_length=3,max_length=150)
    def __init__(self,owner,week_start):super().__init__();self.owner=owner;self.week_start=week_start
    async def on_submit(self,interaction):
        if interaction.user.id!=self.owner or not allowed_guild(interaction) or not manager(interaction):
            return await interaction.response.send_message('Manager-only.',ephemeral=True)
        saved=payroll_week_status(interaction.guild_id,self.week_start)
        if not saved or saved['status']!='approved':
            return await interaction.response.send_message('This week must be approved and not already marked paid.',ephemeral=True)
        current=payroll_week_data(interaction.guild_id,self.week_start)
        if not approved_week_financials_match(interaction.guild_id,self.week_start,current):
            return await interaction.response.send_message('Payroll financial/source records changed since approval. Reconcile before Mark Paid.',ephemeral=True)
        balances=payroll_balances(interaction.guild_id,self.week_start,current)
        missing=[v for v in balances.values() if v['due']!=0]
        if missing:
            return await interaction.response.send_message(
                'Cannot mark paid: every employee must have a recorded payment matching their earnings. '
                'Use Record Payment for transfers, including early payments. '
                f'{len(missing)} employee(s) have a nonzero balance.',ephemeral=True)
        reference=str(self.reference.value).strip();when=now_iso()
        with DB:
            changed=DB.execute('''UPDATE payroll_periods SET status='paid',paid_at=?,paid_by=?,payment_reference=?
                WHERE guild_id=? AND week_start=? AND status='approved' AND fingerprint=?''',
                (when,interaction.user.id,reference,interaction.guild_id,self.week_start,saved['fingerprint'])).rowcount
            if changed:
                DB.execute('INSERT INTO payroll_period_audit(guild_id,week_start,actor_id,action,occurred_at,details) VALUES(?,?,?,?,?,?)',
                    (interaction.guild_id,self.week_start,interaction.user.id,'marked_paid',when,reference))
        await interaction.response.send_message('✅ Marked PAID in ETBeastDemo ledger.' if changed else 'Already marked paid or record changed.',ephemeral=True)


@bot.tree.command(name='weeklypayroll',description='Manager: review, approve and mark an exact weekly pay period')
@app_commands.guild_only()
async def weeklypayroll(interaction:discord.Interaction):
    if not manager(interaction) or not allowed_guild(interaction):
        return await interaction.response.send_message('Manager permission required.',ephemeral=True)
    today=datetime.now(PAYROLL_TZ).date();sunday=payroll_week_start(today)
    start=(sunday-timedelta(days=7)).isoformat()  # previous completed week by default
    week=payroll_week_data(interaction.guild_id,start)
    await interaction.response.defer(ephemeral=True)
    await preload_employee_names(interaction.guild,[int(x) for x in week['employees']])
    await interaction.followup.send(embed=payroll_week_embed(interaction.guild,week),
      view=WeeklyPayrollView(interaction.user.id,interaction.guild,start),ephemeral=True)

@bot.tree.command(name='streamreport', description='Manager: export approved and pending stream segments with guarantee details')
@app_commands.guild_only()
@app_commands.describe(days='Lookback days (1–90)')
async def streamreport(interaction: discord.Interaction, days: app_commands.Range[int,1,90] = 7):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.',ephemeral=True)
        return
    cutoff=(datetime.now(timezone.utc)-timedelta(days=days)).isoformat()
    rows=DB.execute("""SELECT c.id,c.shift_id,c.user_id,c.submitted_at,c.stream_type,
                      c.revenue_cents,c.sales_per_hour_cents,c.character_hours,c.pack_size,c.break_count,c.status,
                      c.commission_cents,c.guarantee_topup_cents,s.work_type,s.clock_in,s.clock_out
                      FROM commissions c JOIN shifts s ON c.shift_id=s.id
                      WHERE c.guild_id=? AND c.submitted_at>=? ORDER BY c.submitted_at""",
                    (interaction.guild_id,cutoff)).fetchall()
    buff=io.StringIO();writer=csv.writer(buff)
    writer.writerow(['commission_id','shift_id','employee_id','submitted_at_utc','stream_type',
                     'shift_work_type','shift_start','shift_end','revenue','pack_size','break_count','legacy_sales_per_hour',
                     'character_hours','status','commission_2_percent','character_topup','approved_variable_pay'])
    for row in rows:
        approved=row['status']=='approved'
        writer.writerow([row['id'],row['shift_id'],row['user_id'],row['submitted_at'],row['stream_type'],
                         row['work_type'],row['clock_in'],row['clock_out'],
                         money(row['revenue_cents']) if row['revenue_cents'] is not None else '',
                         row['pack_size'] or '',row['break_count'] or '',
                         money(row['sales_per_hour_cents']) if row['sales_per_hour_cents'] is not None else '',
                         row['character_hours'] or '',row['status'],money(row['commission_cents']),
                         money(row['guarantee_topup_cents']),
                         money(row['commission_cents']+row['guarantee_topup_cents']) if approved else money(0)])
    await interaction.response.send_message(
        f'{len(rows)} segment submissions. Only approved entries are included in payroll; verify screenshots and shift hours. ',
        file=discord.File(io.BytesIO(buff.getvalue().encode('utf-8-sig')),filename='etbeast_demo_stream_segments.csv'),
        ephemeral=True)


# -------- V8: boss-friendly private dashboard --------
from zoneinfo import ZoneInfo

BOSS_TZ = ZoneInfo('America/New_York')


async def preload_employee_names(guild, user_ids):
    """Fetch uncached members for manager reports; do not mistake cache misses for departures.

    Discord's member cache may still be cold after reconnecting. Limit HTTP calls
    and retain stable user IDs in the database regardless of nickname changes.
    """
    if guild is None:
        return
    ids = {int(uid) for uid in user_ids if uid is not None}
    missing = [uid for uid in ids if guild.get_member(uid) is None]
    for uid in missing[:50]:
        try:
            # fetch_member returns the current server member even if not cached.
            member = await guild.fetch_member(uid)
            _employee_name_cache[(guild.id, uid)] = member.display_name
        except discord.NotFound:
            # The employee may have left, but still retain a mention as a safe fallback.
            continue
        except (discord.Forbidden, discord.HTTPException):
            logging.warning('Could not resolve nickname for user %s in guild %s', uid, guild.id)
            continue


_employee_name_cache = {}


def employee_name(guild, user_id):
    """Use the live server nickname; never label a cache miss 'Former member'."""
    uid = int(user_id)
    member = guild.get_member(uid) if guild else None
    if member:
        return discord.utils.escape_markdown(member.display_name)
    if guild and (guild.id, uid) in _employee_name_cache:
        return discord.utils.escape_markdown(_employee_name_cache[(guild.id, uid)])
    return f'Employee (ID {uid})'


async def resolve_employee_name(guild, user_id):
    """Resolve the live server nickname before publishing a Discord message."""
    if guild is None:
        return employee_name(guild, user_id)
    await preload_employee_names(guild, [user_id])
    return employee_name(guild, user_id)


@bot.event
async def on_member_update(before, after):
    _employee_name_cache[(after.guild.id, after.id)] = after.display_name


def friendly_time(value):
    """Convert stored HH:MM to familiar 12-hour Eastern display."""
    if not value:
        return '—'
    return datetime.strptime(value, '%H:%M').strftime('%I:%M %p').lstrip('0')


def friendly_window(a, b):
    return f'{friendly_time(a)}–{friendly_time(b)}'


def boss_local(ts):
    if not ts:
        return 'Still clocked in'
    return datetime.fromisoformat(ts).astimezone(BOSS_TZ).strftime('%Y-%m-%d %I:%M %p')


def boss_time_to_iso(value):
    """Parse local NY time, rejecting nonexistent or ambiguous DST times."""
    try:
        naive = datetime.strptime(value.strip(), '%Y-%m-%d %H:%M')
    except ValueError as exc:
        raise ValueError('Use YYYY-MM-DD HH:MM in Eastern time, e.g. 2030-01-12 15:30.') from exc
    a = naive.replace(tzinfo=BOSS_TZ, fold=0)
    b = naive.replace(tzinfo=BOSS_TZ, fold=1)
    if a.utcoffset() != b.utcoffset():
        raise ValueError('Ambiguous or nonexistent daylight-saving time. Use /editshift or /addshift with explicit UTC offset.')
    if a.astimezone(timezone.utc).astimezone(BOSS_TZ).replace(tzinfo=None) != naive:
        raise ValueError('This local time does not exist due to daylight-saving time. Use the slash command with UTC offset.')
    return a.isoformat()


def boss_embed(guild_id):
    cfg = settings_for(guild_id)
    active = DB.execute('SELECT count(*) AS n FROM shifts WHERE guild_id=? AND clock_out IS NULL',(guild_id,)).fetchone()['n']
    pending = DB.execute("SELECT count(*) AS n FROM commissions WHERE guild_id=? AND status='pending'",(guild_id,)).fetchone()['n']
    incidents = DB.execute("SELECT count(*) AS n FROM incident_fines WHERE guild_id=? AND status='pending'",(guild_id,)).fetchone()['n']
    e = discord.Embed(title='ETBeastDemo • Manager Dashboard', description='Tap a button. Only you can see this menu.', color=discord.Color.blurple())
    e.add_field(name='On the clock',value=str(active))
    e.add_field(name='Commissions awaiting review',value=str(pending))
    e.add_field(name='Incidents awaiting review',value=str(incidents))
    e.add_field(name='Streaming pay',value='Example hourly rate + commission')
    e.add_field(name='Packing pay',value='Example hourly rate • per-employee rates supported')
    e.add_field(name='Character Break pay',value='Example minimum for verified segment')
    e.add_field(name='Legacy default rate',value=f'{money(cfg["hourly_rate_cents"])} + {Decimal(cfg["commission_rate"])*100:g}% (historical/legacy only)')
    e.set_footer(text='Eastern time • Payroll totals require independent review • Fine records never deduct wages')
    return e


async def run_boss_command(interaction, command, **kwargs):
    obj = bot.tree.get_command(command)
    if obj is None:
        await interaction.response.send_message('Command unavailable. Contact the bot administrator.',ephemeral=True)
        return
    await obj.callback(interaction, **kwargs)


class BossConfirmation(discord.ui.View):
    def __init__(self, owner_id, command, kwargs):
        super().__init__(timeout=180)
        self.owner_id = owner_id
        self.command = command
        self.kwargs = kwargs

    async def interaction_check(self, interaction):
        if interaction.user.id != self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            await interaction.response.send_message('This approval belongs to another manager.',ephemeral=True)
            return False
        return True

    @discord.ui.button(label='Confirm and save',style=discord.ButtonStyle.success,emoji='✅')
    async def confirm(self,interaction,button):
        if getattr(self, '_used', False):
            await interaction.response.send_message('Already processed.', ephemeral=True)
            return
        self._used = True
        for child in self.children:
            child.disabled = True
        # send a separate private response from existing audited slash-command implementation
        await run_boss_command(interaction,self.command,**self.kwargs)

    @discord.ui.button(label='Cancel',style=discord.ButtonStyle.secondary,emoji='✖️')
    async def cancel(self,interaction,button):
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content='Cancelled; nothing was changed.',view=None,embed=None)


class BossAmountModal(discord.ui.Modal):
    def __init__(self, member, kind):
        super().__init__(title=('Award employee bonus' if kind=='bonus' else 'Record incident for review'),timeout=300)
        self.member=member
        self.kind=kind
        self.amount=discord.ui.TextInput(label='Amount in dollars',placeholder='25.00',max_length=15)
        self.reason=discord.ui.TextInput(label='Reason',style=discord.TextStyle.paragraph,max_length=250,required=True)
        self.add_item(self.amount)
        self.add_item(self.reason)

    async def on_submit(self,interaction):
        try:
            amt=parse_nonnegative(str(self.amount.value),'100000')
            if amt<=0:
                raise ValueError('Amount must be greater than zero.')
        except (ValueError,InvalidOperation):
            await interaction.response.send_message('Please enter a positive dollar amount such as 25.00.',ephemeral=True)
            return
        if self.kind=='bonus':
            kwargs={'employee':self.member,'amount':str(amt),'reason':str(self.reason.value)}
            summary=f'**Approve bonus?**\nEmployee: {self.member.mention}\nAmount: ${amt:,.2f}\nReason: {self.reason.value}'
            cmd='bonus'
        else:
            event=datetime.now(BOSS_TZ).strftime('%Y-%m-%d')
            kwargs={'employee':self.member,'amount':str(amt),'reason':str(self.reason.value),'event_date':event}
            summary=(f'**Record an incident?**\nEmployee: {self.member.mention}\nProposed amount: ${amt:,.2f}\n'
                     f'Reason: {self.reason.value}\nDate: {event}\n**No wage deduction will be made.**')
            cmd='fine'
        await interaction.response.send_message(summary,view=BossConfirmation(interaction.user.id,cmd,kwargs),ephemeral=True,
                                                allowed_mentions=discord.AllowedMentions.none())


class BossShiftModal(discord.ui.Modal):
    def __init__(self, action, member=None, row=None):
        super().__init__(title=('Correct employee shift' if action=='editshift' else 'Add missed employee shift'),timeout=300)
        self.action=action
        self.member=member
        self.row=row
        self.start=discord.ui.TextInput(label='Clock-in (Eastern, YYYY-MM-DD HH:MM)',max_length=16,
                    default=(datetime.fromisoformat(row['clock_in']).astimezone(BOSS_TZ).strftime('%Y-%m-%d %H:%M') if row else None),
                    placeholder='2030-01-10 10:00')
        self.end=discord.ui.TextInput(label='Clock-out (Eastern, YYYY-MM-DD HH:MM)',max_length=16,
                    default=(datetime.fromisoformat(row['clock_out']).astimezone(BOSS_TZ).strftime('%Y-%m-%d %H:%M') if row and row['clock_out'] else None),
                    placeholder='2030-01-10 15:00')
        self.reason=discord.ui.TextInput(label='Correction reason (at least 5 characters)',style=discord.TextStyle.paragraph,max_length=300)
        self.add_item(self.start);self.add_item(self.end);self.add_item(self.reason)

    async def on_submit(self,interaction):
        try:
            start=boss_time_to_iso(str(self.start.value));end=boss_time_to_iso(str(self.end.value))
            if len(str(self.reason.value).strip())<5:
                raise ValueError('Enter a reason containing at least 5 characters.')
            if self.action=='editshift':
                owner_id=self.row['user_id'];exclude=self.row['id']
            else:
                owner_id=self.member.id;exclude=None
            validate_interval(interaction.guild_id,owner_id,parse_shift_time(start),parse_shift_time(end),exclude)
        except ValueError as exc:
            await interaction.response.send_message(f'❌ {exc}',ephemeral=True)
            return
        if self.action=='editshift':
            kwargs={'shift_id':self.row['id'],'clock_in':start,'clock_out':end,'reason':str(self.reason.value)}
            summary=f'**Confirm correction to shift #{self.row["id"]}** for <@{owner_id}>\n'
        else:
            kwargs={'employee':self.member,'clock_in':start,'clock_out':end,'reason':str(self.reason.value)}
            summary=f'**Confirm new shift** for <@{owner_id}>\n'
        summary+=(f'Clock-in: {boss_local(start)} ET\nClock-out: {boss_local(end)} ET\n'
                  f'Hours: {(parse_shift_time(end)-parse_shift_time(start)).total_seconds()/3600:.2f}\nReason: {self.reason.value}')
        await interaction.response.send_message(summary,view=BossConfirmation(interaction.user.id,self.action,kwargs),ephemeral=True,
                                                allowed_mentions=discord.AllowedMentions.none())


# v15: All manager dashboard time edits use dropdowns rather than typed times.
# Internal values are still 24h, date-aware ISO timestamps to protect stored records.
class ManagerDateSelect(discord.ui.Select):
    def __init__(self, parent, key, row):
        self.key=key
        center=parent.anchor
        choices=[center+timedelta(days=i) for i in range(-14,15)]
        if parent.action=='assign':
            choices=[datetime.now(BOSS_TZ).date()+timedelta(days=i) for i in range(25)]
        # Select menu maximum 25 choices.
        choices=choices[:25]
        if parent.dates[key] not in choices:
            choices[0]=parent.dates[key]
        options=[discord.SelectOption(label=x.strftime('%a, %b %d, %Y'),value=x.isoformat(),default=(x==parent.dates[key])) for x in choices]
        super().__init__(placeholder=('Shift date' if key=='start' else 'Clock-out date'),options=options,row=row)
    async def callback(self,interaction):
        parent=self.view
        if not await parent.authorize(interaction):return
        parent.dates[self.key]=date.fromisoformat(self.values[0])
        if parent.action=='assign':parent.dates['end']=parent.dates['start']
        await interaction.response.edit_message(content=parent.summary(),view=ManagerDateView.from_state(parent))


class ManagerShiftTypeSelect(discord.ui.Select):
    def __init__(self,parent):
        super().__init__(placeholder='Work type: Streaming or Packing',row=3,options=[
            discord.SelectOption(label='Streaming • employee rate',value='streaming',default=parent.work_type=='streaming'),
            discord.SelectOption(label='Packing • employee rate',value='packing',default=parent.work_type=='packing')])
    async def callback(self,interaction):
        parent=self.view
        if not await parent.authorize(interaction):return
        parent.work_type=self.values[0]
        await interaction.response.edit_message(content=parent.summary(),view=ManagerDateView.from_state(parent))


class ManagerDateView(discord.ui.View):
    def __init__(self,owner_id,action,member=None,shift=None,dates=None,times=None,work_type=None):
        super().__init__(timeout=600)
        self.owner_id=owner_id;self.action=action;self.member=member;self.shift=shift
        self.work_type=work_type or (shift['work_type'] if shift and shift['work_type'] in ('streaming','packing') else 'streaming')
        if shift:
            start=datetime.fromisoformat(shift['clock_in']).astimezone(BOSS_TZ)
            end=datetime.fromisoformat(shift['clock_out']).astimezone(BOSS_TZ) if shift['clock_out'] else start+timedelta(hours=4)
        else:
            start=datetime.now(BOSS_TZ).replace(minute=0,second=0,microsecond=0)
            end=start+timedelta(hours=4)
        self.anchor=start.date()
        self.dates=dates or {'start':start.date(),'end':end.date()}
        self.times=times or {'start':start.strftime('%H:%M'),'end':end.strftime('%H:%M')}
        self.add_item(ManagerDateSelect(self,'start',0))
        if action!='assign':
            self.add_item(ManagerDateSelect(self,'end',1))
            self.add_item(ManagerShiftTypeSelect(self))
    @classmethod
    def from_state(cls,other):
        v=cls(other.owner_id,other.action,other.member,other.shift,other.dates.copy(),other.times.copy(),other.work_type)
        v.anchor=other.anchor
        return v
    async def authorize(self,interaction):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            await interaction.response.send_message('Manager access required.',ephemeral=True);return False
        return True
    def summary(self):
        return f'**Choose date(s)** • {employee_name(bot.get_guild(self.shift["guild_id"]),self.shift["user_id"]) if self.shift else self.member.display_name}\nStart: {self.dates["start"]}\nEnd: {self.dates["end"]}\nNext choose times.'
    @discord.ui.button(label='Next: Choose Times',style=discord.ButtonStyle.primary,row=2)
    async def next_button(self,interaction,button):
        if not await self.authorize(interaction):return
        await interaction.response.edit_message(content='Choose start and end using AM/PM dropdowns.',view=ManagerTimesView(self))


class ManagerTimeSelect(discord.ui.Select):
    def __init__(self,parent,key,part,row):
        self.key=key;self.part=part
        current=parent.times[key].split(':')[0 if part=='hour' else 1]
        opts=[]
        if part=='hour':
            for n in range(24):
                val=f'{n:02d}'
                opts.append(discord.SelectOption(
                    label=f'{n%12 or 12} {"AM" if n<12 else "PM"}',value=val,default=val==current))
        else:
            # Discord permits at most 25 options in one select. Show 20 exact
            # minutes at a time, with navigation entries when needed.
            page=parent.minute_pages[key]
            for n in range(page*20,min(page*20+20,60)):
                val=f'{n:02d}'
                opts.append(discord.SelectOption(label=f':{val}',value=val,default=val==current))
            if page>0:
                opts.append(discord.SelectOption(label='← Earlier minutes',value='__prev__'))
            if page<2:
                opts.append(discord.SelectOption(label='Later minutes →',value='__next__'))
        super().__init__(placeholder=f'{key.title()} {part}',options=opts,row=row)

    async def callback(self,interaction):
        parent=self.view
        if not await parent.authorize(interaction):return
        value=self.values[0]
        if self.part=='minute' and value in ('__prev__','__next__'):
            change=-1 if value=='__prev__' else 1
            parent.minute_pages[self.key]=max(0,min(2,parent.minute_pages[self.key]+change))
        else:
            pieces=parent.times[self.key].split(':')
            pieces[0 if self.part=='hour' else 1]=value
            parent.times[self.key]=':'.join(pieces)
        await interaction.response.edit_message(content=parent.summary(),view=ManagerTimesView.from_state(parent))


# v37: Exact-minute fallback for actual payroll shift corrections.
# This deliberately does not change scheduled shift or availability controls.
class ManagerExactMinutesModal(discord.ui.Modal, title='Set exact shift minutes'):
    def __init__(self, times_view):
        super().__init__(timeout=300)
        self.times_view=times_view
        self.start_minute=discord.ui.TextInput(
            label='Clock-in minute (00-59)',placeholder='17',min_length=1,max_length=2,
            default=times_view.times['start'].split(':')[1])
        self.end_minute=discord.ui.TextInput(
            label='Clock-out minute (00-59)',placeholder='43',min_length=1,max_length=2,
            default=times_view.times['end'].split(':')[1])
        self.add_item(self.start_minute)
        self.add_item(self.end_minute)

    async def on_submit(self, interaction):
        old=self.times_view
        if not await old.authorize(interaction):
            return
        values=(str(self.start_minute.value).strip(),str(self.end_minute.value).strip())
        if any(not value.isdigit() or not 0<=int(value)<=59 for value in values):
            await interaction.response.send_message(
                'Enter whole-number minutes from 00 to 59 for both fields.',ephemeral=True)
            return
        new=ManagerTimesView.from_state(old)
        for key,minute in zip(('start','end'),values):
            hour=new.times[key].split(':')[0]
            new.times[key]=f'{hour}:{int(minute):02d}'
            new.minute_pages[key]=int(minute)//20
        await interaction.response.send_message(
            'Exact minutes updated. Review both times, then choose **Next: Review & Save**.',
            view=new,ephemeral=True)


class ManagerTimesView(discord.ui.View):
    def __init__(self,dateview):
        super().__init__(timeout=600)
        self.owner_id=dateview.owner_id;self.action=dateview.action;self.member=dateview.member;self.shift=dateview.shift
        self.dates=dateview.dates.copy();self.times=dateview.times.copy();self.anchor=dateview.anchor;self.work_type=dateview.work_type
        self.minute_pages=getattr(dateview,'minute_pages',None)
        self.minute_pages=self.minute_pages.copy() if self.minute_pages is not None else {
            key:int(self.times[key].split(':')[1])//20 for key in ('start','end')}
        for row,(key,part) in enumerate([('start','hour'),('start','minute'),('end','hour'),('end','minute')]):
            self.add_item(ManagerTimeSelect(self,key,part,row))
    @classmethod
    def from_state(cls,other):return cls(other)
    async def authorize(self,interaction):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            await interaction.response.send_message('Manager access required.',ephemeral=True);return False
        return True
    def summary(self):
        return f'**Confirm times**\nStart: {self.dates["start"]} {friendly_time(self.times["start"])}\nEnd: {self.dates["end"]} {friendly_time(self.times["end"])} • Eastern'
    @discord.ui.button(label='Set Exact Minutes (00–59)',style=discord.ButtonStyle.secondary,row=4)
    async def exact_minutes_button(self,interaction,button):
        if not await self.authorize(interaction):return
        await interaction.response.send_modal(ManagerExactMinutesModal(self))

    @discord.ui.button(label='Next: Review & Save',style=discord.ButtonStyle.success,row=4)
    async def save_button(self,interaction,button):
        if not await self.authorize(interaction):return
        try:
            a=self.dates['start'].isoformat()+' '+self.times['start']
            b=self.dates['end'].isoformat()+' '+self.times['end']
            if self.action=='assign':
                d=date_from_text(self.dates['start'].isoformat()).isoformat()
                start,end=validate_window(self.times['start'],self.times['end'])
                await interaction.response.defer(ephemeral=True,thinking=True)
                return await assign_and_notify(interaction,self.member,d,start,end)
            start=boss_time_to_iso(a);end=boss_time_to_iso(b)
            uid=self.shift['user_id'] if self.shift else self.member.id
            validate_interval(interaction.guild_id,uid,parse_shift_time(start),parse_shift_time(end),self.shift['id'] if self.shift else None)
        except ValueError as exc:
            return await interaction.response.send_message(f'⚠️ {exc}',ephemeral=True)
        await interaction.response.send_modal(ManagerShiftReasonModal(self.action,self.member,self.shift,start,end,self.work_type))


class ManagerShiftReasonModal(discord.ui.Modal,title='Explain shift correction'):
    reason=discord.ui.TextInput(label='Reason for correction',min_length=5,max_length=300,style=discord.TextStyle.paragraph)
    def __init__(self,action,member,shift,start,end,work_type):
        super().__init__();self.action=action;self.member=member;self.shift=shift;self.start=start;self.end=end;self.work_type=work_type
    async def on_submit(self,interaction):
        if not manager(interaction) or not allowed_guild(interaction):return await interaction.response.send_message('Manager only.',ephemeral=True)
        if self.action=='editshift':
            kwargs={'shift_id':self.shift['id'],'clock_in':self.start,'clock_out':self.end,'reason':str(self.reason.value),'work_type':app_commands.Choice(name=self.work_type.title(),value=self.work_type)}
        else:
            kwargs={'employee':self.member,'clock_in':self.start,'clock_out':self.end,'reason':str(self.reason.value),'work_type':app_commands.Choice(name=self.work_type.title(),value=self.work_type)}
        if self.action=='editshift':
            preview_rate=shift_correction_rate(interaction.guild_id,self.shift['user_id'],
                self.shift['work_type'],self.shift['hourly_rate_cents'],self.work_type)
        else:
            preview_rate=shift_rate_for_employee(interaction.guild_id,self.member.id,self.work_type)
        summary=f'**Review {self.action}**\n{boss_local(self.start)} → {boss_local(self.end)} ET\nWork type: {self.work_type.title()} • {money(preview_rate)}/hr\nReason: {self.reason.value}'
        await interaction.response.send_message(summary,view=BossConfirmation(interaction.user.id,self.action,kwargs),ephemeral=True)


class BossMemberSelect(discord.ui.UserSelect):
    def __init__(self,action,owner_id):
        super().__init__(placeholder='Choose employee from Discord',min_values=1,max_values=1)
        self.action=action;self.owner_id=owner_id

    async def callback(self,interaction):
        if interaction.user.id != self.owner_id or not manager(interaction):
            await interaction.response.send_message('Only the requesting manager can use this.',ephemeral=True);return
        selected=self.values[0]
        member=(selected if isinstance(selected, discord.Member) else interaction.guild.get_member(selected.id))
        if member is None:
            await interaction.response.send_message('Please select an employee who belongs to this server.',ephemeral=True);return
        if self.action=='addshift':
            await interaction.response.send_message('Select shift date(s):',view=ManagerDateView(interaction.user.id,'addshift',member=member),ephemeral=True)
        else:
            await interaction.response.send_modal(BossAmountModal(member,self.action))


class BossMemberPicker(discord.ui.View):
    def __init__(self,action,owner_id):
        super().__init__(timeout=180)
        self.add_item(BossMemberSelect(action,owner_id))


class BossShiftSelect(discord.ui.Select):
    def __init__(self,owner_id,rows):
        options=[]
        for r in rows:
            label=f'#{r["id"]} • {employee_name(bot.get_guild(r["guild_id"]),r["user_id"])} • {boss_local(r["clock_in"])}'
            options.append(discord.SelectOption(label=label[:100],value=str(r['id']),
                        description=('OPEN - forgot clock-out?' if not r['clock_out'] else 'Completed shift')))
        super().__init__(placeholder='Choose shift to correct',options=options)
        self.owner_id=owner_id

    async def callback(self,interaction):
        if interaction.user.id != self.owner_id or not manager(interaction):
            await interaction.response.send_message('Only the requesting manager can use this.',ephemeral=True);return
        row=DB.execute('SELECT * FROM shifts WHERE id=? AND guild_id=?',(int(self.values[0]),interaction.guild_id)).fetchone()
        if not row:
            await interaction.response.send_message('Shift no longer exists.',ephemeral=True);return
        await interaction.response.send_message('Select shift date(s):',view=ManagerDateView(interaction.user.id,'editshift',shift=row),ephemeral=True)


class BossShiftPicker(discord.ui.View):
    def __init__(self,owner_id,rows):
        super().__init__(timeout=180)
        self.add_item(BossShiftSelect(owner_id,rows))


class BossChannelSelect(discord.ui.ChannelSelect):
    def __init__(self,owner_id,setting):
        super().__init__(placeholder='Choose private channel',channel_types=[discord.ChannelType.text])
        self.owner_id=owner_id;self.setting=setting

    async def callback(self,interaction):
        if interaction.user.id != self.owner_id or not manager(interaction):
            await interaction.response.send_message('Not authorized.',ephemeral=True);return
        channel=interaction.guild.get_channel(self.values[0].id)
        if not isinstance(channel,discord.TextChannel):
            await interaction.response.send_message('Select a regular text channel.',ephemeral=True);return
        if not channel.permissions_for(interaction.guild.me).send_messages:
            await interaction.response.send_message('The bot cannot post in that channel. Update channel permissions first.',ephemeral=True);return
        save_guild_setting(interaction.guild_id,interaction.user.id,self.setting,channel.id)
        await interaction.response.send_message(f'✅ Channel saved: {channel.mention}. Verify the channel is restricted to managers.',ephemeral=True)


class BossChannelPicker(discord.ui.View):
    def __init__(self,owner_id,setting):
        super().__init__(timeout=180)
        self.add_item(BossChannelSelect(owner_id,setting))


class BossRecordReasonModal(discord.ui.Modal):
    def __init__(self,kind,row):
        super().__init__(title=('Void a bonus' if kind=='voidbonus' else 'Review an incident'),timeout=300)
        self.kind=kind;self.row=row
        self.reason=discord.ui.TextInput(label=('Why should this bonus be voided?' if kind=='voidbonus' else 'Review decision / explanation'),style=discord.TextStyle.paragraph,max_length=250)
        self.add_item(self.reason)

    async def on_submit(self,interaction):
        note=str(self.reason.value).strip()
        if not note:
            await interaction.response.send_message('Please explain the decision.',ephemeral=True);return
        if self.kind=='voidbonus':
            kwargs={'bonus_id':self.row['id'],'reason':note}
            summary=f'**Void bonus #{self.row["id"]}?**\nValue: {money(self.row["amount_cents"])}\nReason: {note}'
        else:
            kwargs={'fine_id':self.row['id'],'status':app_commands.Choice(name='Dismissed',value='dismissed'),'note':note}
            summary=f'**Dismiss incident #{self.row["id"]}?**\nReason: {note}\nThis never changes employee pay.'
        await interaction.response.send_message(summary,view=BossConfirmation(interaction.user.id,self.kind,kwargs),ephemeral=True)


class BossRecordSelect(discord.ui.Select):
    def __init__(self,kind,owner_id,rows):
        self.kind=kind;self.owner_id=owner_id
        label=('Choose bonus to void' if kind=='voidbonus' else 'Choose incident to dismiss')
        options=[discord.SelectOption(label=(f'#{r["id"]} • {money(r["amount_cents"])} • {employee_name(bot.get_guild(r["guild_id"]),r["employee_id"])}' if kind=='voidbonus' else
                                              f'#{r["id"]} • {money(r["proposed_amount_cents"])} • {employee_name(bot.get_guild(r["guild_id"]),r["employee_id"])}'),value=str(r['id'])) for r in rows]
        super().__init__(placeholder=label,options=options)

    async def callback(self,interaction):
        if interaction.user.id != self.owner_id or not manager(interaction):
            await interaction.response.send_message('Not authorized.',ephemeral=True);return
        table='bonuses' if self.kind=='voidbonus' else 'incident_fines'
        row=DB.execute(f'SELECT * FROM {table} WHERE guild_id=? AND id=?',(interaction.guild_id,int(self.values[0]))).fetchone()
        if not row:
            await interaction.response.send_message('Record not found.',ephemeral=True);return
        await interaction.response.send_modal(BossRecordReasonModal(self.kind,row))


class BossRecordPicker(discord.ui.View):
    def __init__(self,kind,owner_id,rows):
        super().__init__(timeout=180)
        self.add_item(BossRecordSelect(kind,owner_id,rows))



class BossDashboard(discord.ui.View):
    def __init__(self,owner_id):
        super().__init__(timeout=600)
        self.owner_id=owner_id

    async def interaction_check(self,interaction):
        if interaction.user.id != self.owner_id or not allowed_guild(interaction) or not manager(interaction):
            await interaction.response.send_message('Manager-only: open your own /dashboard.',ephemeral=True)
            return False
        return True

    @discord.ui.button(label='Hours',emoji='🕒',style=discord.ButtonStyle.primary,row=0)
    async def hours_button(self,interaction,button):
        await run_boss_command(interaction,'hours',days=7,employee=None)

    @discord.ui.button(label='Performance',emoji='📈',style=discord.ButtonStyle.primary,row=0)
    async def performance_button(self,interaction,button):
        await run_boss_command(interaction,'performance',days=7,employee=None)

    @discord.ui.button(label='Payroll Review',emoji='💵',style=discord.ButtonStyle.primary,row=0)
    async def payroll_button(self,interaction,button):
        await run_boss_command(interaction,'payrollreview')

    @discord.ui.button(label='Settings',emoji='⚙️',style=discord.ButtonStyle.secondary,row=0)
    async def settings_button(self,interaction,button):
        await interaction.response.send_message(embed=configuration_embed(interaction.guild_id),view=SettingsView(),ephemeral=True)

    @discord.ui.button(label='Award Bonus',emoji='🎁',style=discord.ButtonStyle.success,row=1)
    async def bonus_button(self,interaction,button):
        await interaction.response.send_message('Pick employee for bonus:',view=BossMemberPicker('bonus',interaction.user.id),ephemeral=True)

    @discord.ui.button(label='Record Incident',emoji='📋',style=discord.ButtonStyle.secondary,row=1)
    async def fine_button(self,interaction,button):
        await interaction.response.send_message('Pick employee for incident record (not a payroll deduction):',view=BossMemberPicker('fine',interaction.user.id),ephemeral=True)

    @discord.ui.button(label='Fix Shift',emoji='🛠️',style=discord.ButtonStyle.secondary,row=1)
    async def edit_button(self,interaction,button):
        rows=DB.execute('SELECT * FROM shifts WHERE guild_id=? ORDER BY id DESC LIMIT 25',(interaction.guild_id,)).fetchall()
        if not rows:
            await interaction.response.send_message('No shifts recorded yet.',ephemeral=True);return
        await interaction.response.send_message('Choose a recent shift to correct:',view=BossShiftPicker(interaction.user.id,rows),ephemeral=True)

    @discord.ui.button(label='Add Shift',emoji='➕',style=discord.ButtonStyle.secondary,row=1)
    async def add_button(self,interaction,button):
        await interaction.response.send_message('Pick employee for missed shift:',view=BossMemberPicker('addshift',interaction.user.id),ephemeral=True)

    @discord.ui.button(label='Bonuses',emoji='💰',style=discord.ButtonStyle.secondary,row=2)
    async def bonuses_button(self,interaction,button):
        await run_boss_command(interaction,'bonuses',days=30,employee=None)

    @discord.ui.button(label='Incidents',emoji='🗂️',style=discord.ButtonStyle.secondary,row=2)
    async def incidents_button(self,interaction,button):
        await run_boss_command(interaction,'fines',employee=None,days=30)

    @discord.ui.button(label='Backup DB',emoji='🗄️',style=discord.ButtonStyle.secondary,row=2)
    async def backup_button(self,interaction,button):
        await run_boss_command(interaction,'backup')

    @discord.ui.button(label='Refresh',emoji='🔄',style=discord.ButtonStyle.secondary,row=2)
    async def refresh_button(self,interaction,button):
        await interaction.response.edit_message(embed=boss_embed(interaction.guild_id),view=self)

    @discord.ui.button(label='Set Log Channel',emoji='📢',style=discord.ButtonStyle.secondary,row=3)
    async def logs_button(self,interaction,button):
        await interaction.response.send_message('Select the private channel for clock logs:',view=BossChannelPicker(interaction.user.id,'log_channel_id'),ephemeral=True)

    @discord.ui.button(label='Set Review Channel',emoji='📷',style=discord.ButtonStyle.secondary,row=3)
    async def review_button(self,interaction,button):
        await interaction.response.send_message('Select the private channel for commission approvals:',view=BossChannelPicker(interaction.user.id,'review_channel_id'),ephemeral=True)

    @discord.ui.button(label='Set Performance Channel',emoji='🏆',style=discord.ButtonStyle.secondary,row=3)
    async def perf_channel_button(self,interaction,button):
        await interaction.response.send_message('Select the private performance channel:',view=BossChannelPicker(interaction.user.id,'performance_channel_id'),ephemeral=True)



    @discord.ui.button(label='Scheduling',emoji='📅',style=discord.ButtonStyle.secondary,row=3)
    async def schedule_button(self,interaction,button):
        await interaction.response.send_message('Manage employee availability, assign shifts, or configure the schedule channel:',view=SchedulingManagerView(interaction.user.id),ephemeral=True)

    @discord.ui.button(label='Void Bonus',emoji='↩️',style=discord.ButtonStyle.secondary,row=4)
    async def _boss_void_bonus(self,interaction,button):
        rows=DB.execute('SELECT * FROM bonuses WHERE guild_id=? AND voided_at IS NULL ORDER BY id DESC LIMIT 25',(interaction.guild_id,)).fetchall()
        if not rows:
            await interaction.response.send_message('No active bonuses found.',ephemeral=True);return
        await interaction.response.send_message('Choose the bonus to void:',view=BossRecordPicker('voidbonus',interaction.user.id,rows),ephemeral=True)


    @discord.ui.button(label='Dismiss Incident',emoji='✅',style=discord.ButtonStyle.secondary,row=4)
    async def _boss_dismiss_fine(self,interaction,button):
        rows=DB.execute("SELECT * FROM incident_fines WHERE guild_id=? AND status!='dismissed' ORDER BY id DESC LIMIT 25",(interaction.guild_id,)).fetchall()
        if not rows:
            await interaction.response.send_message('No open incidents found.',ephemeral=True);return
        await interaction.response.send_message('Choose the incident to dismiss:',view=BossRecordPicker('reviewfine',interaction.user.id,rows),ephemeral=True)


    @discord.ui.button(label='Shift Audit CSV',emoji='🔍',style=discord.ButtonStyle.secondary,row=4)
    async def _boss_shift_audit(self,interaction,button):
        await run_boss_command(interaction,'shiftaudit')


    @discord.ui.button(label='Post Leaderboard',emoji='🏆',style=discord.ButtonStyle.secondary,row=4)
    async def _boss_post_leaderboard(self,interaction,button):
        await run_boss_command(interaction,'postperformance',days=7)



@bot.tree.command(name='dashboard',description='Open your ETBeastDemo dashboard (employee or manager)')
@app_commands.guild_only()
async def dashboard(interaction:discord.Interaction):
    if not allowed_guild(interaction) or not employee(interaction):
        await interaction.response.send_message('Employee access required.',ephemeral=True)
        return
    if manager(interaction):
        await interaction.response.send_message(embed=boss_embed(interaction.guild_id),view=ManagerHome(interaction.user.id),ephemeral=True)
    else:
        await interaction.response.send_message(embed=employee_home_embed(interaction),view=EmployeeHome(interaction.user.id),ephemeral=True)


# V9: recurring employee availability and management-approved dated assignments.
# Availability never clocks an employee in or automatically assigns a shift.
SCHEDULE_TZ = ZoneInfo('America/New_York')
DAY_NAMES = ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')
DB.executescript('''
CREATE TABLE IF NOT EXISTS availability (
 guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL, weekday INTEGER NOT NULL,
 start_local TEXT NOT NULL, end_local TEXT NOT NULL, updated_at TEXT NOT NULL,
 PRIMARY KEY (guild_id, user_id, weekday)
);
CREATE TABLE IF NOT EXISTS planned_shifts (
 id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
 user_id INTEGER NOT NULL, work_date TEXT NOT NULL,
 start_local TEXT NOT NULL, end_local TEXT NOT NULL,
 created_by INTEGER NOT NULL, created_at TEXT NOT NULL,
 cancelled_at TEXT, cancelled_by INTEGER, cancellation_reason TEXT
);
CREATE INDEX IF NOT EXISTS planned_shifts_date ON planned_shifts(guild_id,work_date);
CREATE TABLE IF NOT EXISTS schedule_posts (
 guild_id INTEGER NOT NULL, week_start TEXT NOT NULL,
 channel_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
 PRIMARY KEY (guild_id,week_start)
);
CREATE TABLE IF NOT EXISTS schedule_audit (
 id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
 actor_id INTEGER NOT NULL, action TEXT NOT NULL,
 detail TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS schedule_config (
 guild_id INTEGER PRIMARY KEY, channel_id INTEGER NOT NULL
);
''')
DB.commit()


def local_week_start(week=None):
    if week is None:
        d=datetime.now(SCHEDULE_TZ).date()
    else:
        d=date.fromisoformat(week)
    return d - timedelta(days=d.weekday())


def clean_time(value):
    # 24-hour notation, valid increments in minutes.
    try:
        raw=value.strip().upper()
        if raw.endswith((' AM',' PM')):
            return datetime.strptime(raw,'%I:%M %p').strftime('%H:%M')
        hh,mm=raw.split(':')
        if len(hh) not in (1,2) or len(mm)!=2 or not hh.isdigit() or not mm.isdigit():
            raise ValueError
        t=time(int(hh),int(mm))
    except ValueError:
        raise ValueError('Use the dashboard time dropdowns, or enter e.g. 9:30 AM or 5:00 PM.')
    return t.strftime('%H:%M')


def validate_window(start,end):
    a,b=clean_time(start),clean_time(end)
    if a>=b:
        raise ValueError('The end time must be after the start time (same day).')
    return a,b


def date_from_text(value):
    try:
        d=date.fromisoformat(value.strip())
    except ValueError:
        raise ValueError('Use a date like 2030-01-12 (YYYY-MM-DD).')
    today=datetime.now(SCHEDULE_TZ).date()
    if d < today or d > today+timedelta(days=120):
        raise ValueError('Choose a date from today through the next 120 days.')
    return d


def schedule_channel(guild):
    row=DB.execute('SELECT channel_id FROM schedule_config WHERE guild_id=?',(guild.id,)).fetchone()
    if row:
        return guild.get_channel(row['channel_id'])
    return discord.utils.get(guild.text_channels,name='schedule')


def schedule_embed(guild,week):
    week=local_week_start(week)
    end=week+timedelta(days=6)
    e=discord.Embed(title='📅 ETBeastDemo • Confirmed Weekly Schedule',
                    description=f'**{week:%b %d} – {end:%b %d, %Y}**  •  Eastern Time\nOnly shifts assigned by managers appear below. Availability does not mean you are scheduled.',
                    color=discord.Color.blurple())
    for day in range(7):
        d=week+timedelta(days=day)
        rows=DB.execute('''SELECT * FROM planned_shifts WHERE guild_id=? AND work_date=?
                            AND cancelled_at IS NULL ORDER BY start_local,user_id''',(guild.id,d.isoformat())).fetchall()
        value='\n'.join(f'`{friendly_window(r["start_local"], r["end_local"])}` {employee_name(guild, r["user_id"])}' for r in rows) or '*No shifts assigned*'
        e.add_field(name=d.strftime('%A • %b %d'),value=value[:1024],inline=False)
    e.set_footer(text='Times are America/New_York • Shift IDs are for manager changes • Clock in separately when working')
    return e


async def publish_week(guild,week):
    channel=schedule_channel(guild)
    if not isinstance(channel,discord.TextChannel):
        return False
    week=local_week_start(week)
    scheduled_ids=[r['user_id'] for r in DB.execute('SELECT user_id FROM planned_shifts WHERE guild_id=? AND work_date>=? AND work_date<=? AND cancelled_at IS NULL', (guild.id,week.isoformat(),(week+timedelta(days=6)).isoformat())).fetchall()]
    await preload_employee_names(guild, scheduled_ids)
    embed=schedule_embed(guild,week.isoformat())
    row=DB.execute('SELECT * FROM schedule_posts WHERE guild_id=? AND week_start=?',(guild.id,week.isoformat())).fetchone()
    if row and row['channel_id']==channel.id:
        try:
            msg=await channel.fetch_message(row['message_id'])
            await msg.edit(embed=embed,allowed_mentions=discord.AllowedMentions.none())
            return True
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            logging.warning('Weekly schedule message not editable; trying a new post',exc_info=True)
    try:
        msg=await channel.send(embed=embed,allowed_mentions=discord.AllowedMentions.none())
    except (discord.Forbidden,discord.HTTPException):
        logging.exception('Failed posting schedule to guild %s',guild.id)
        return False
    DB.execute('''INSERT INTO schedule_posts (guild_id,week_start,channel_id,message_id) VALUES (?,?,?,?)
                  ON CONFLICT(guild_id,week_start) DO UPDATE SET channel_id=excluded.channel_id,message_id=excluded.message_id''',
               (guild.id,week.isoformat(),channel.id,msg.id))
    DB.commit()
    return True


@tasks.loop(minutes=30)
async def weekly_schedule_refresh():
    for guild in bot.guilds:
        if isinstance(schedule_channel(guild),discord.TextChannel):
            week=local_week_start()
            row=DB.execute('SELECT 1 FROM schedule_posts WHERE guild_id=? AND week_start=?',(guild.id,week.isoformat())).fetchone()
            if not row:
                await publish_week(guild,week.isoformat())


@weekly_schedule_refresh.before_loop
async def before_weekly_schedule_refresh():
    await bot.wait_until_ready()


# V11: Time-select availability UI: day -> dropdowns -> confirm.
# Discord select menus allow up to 25 choices, so hours (24) and
# quarter-hour increments (4) are separate menus.
class AvailabilityTimeSelect(discord.ui.Select):
    def __init__(self, owner_view, field, part, row):
        self.field, self.part = field, part
        current = getattr(owner_view, field)
        selected = current.split(':')[0 if part == 'hour' else 1]
        values = range(24) if part == 'hour' else (0,15,30,45)
        options = []
        for number in values:
            value=f'{number:02d}'
            if part == 'hour':
                display=f'{(number-1)%12+1} {"AM" if number < 12 else "PM"}'
            else:
                display=f':{value}'
            options.append(discord.SelectOption(label=display, value=value, default=(selected==value)))
        friendly='Start' if field == 'start' else 'End'
        kind='hour' if part == 'hour' else 'minutes'
        super().__init__(placeholder=f'{friendly} {kind}',options=options,row=row,min_values=1,max_values=1)

    async def callback(self, interaction: discord.Interaction):
        v=self.view
        if interaction.user.id != v.owner_id:
            await interaction.response.send_message('This selector belongs to another employee.',ephemeral=True)
            return
        pieces=getattr(v,self.field).split(':')
        pieces[0 if self.part == 'hour' else 1]=self.values[0]
        setattr(v,self.field, ':'.join(pieces))
        await interaction.response.edit_message(content=v.description(),view=AvailabilityTimeView(v.owner_id,v.weekday,v.start,v.end))


class AvailabilitySaveButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label='Save Availability',emoji='✅',style=discord.ButtonStyle.success,row=4)

    async def callback(self,interaction:discord.Interaction):
        v=self.view
        if interaction.user.id != v.owner_id or not allowed_guild(interaction) or not employee(interaction):
            await interaction.response.send_message('Employee access required.',ephemeral=True)
            return
        try:
            start,end=validate_window(v.start,v.end)
        except ValueError as exc:
            await interaction.response.send_message(f'⚠️ {exc} Select a later end time and try again.',ephemeral=True)
            return
        DB.execute('''INSERT INTO availability VALUES (?,?,?,?,?,?)
          ON CONFLICT(guild_id,user_id,weekday) DO UPDATE SET
          start_local=excluded.start_local,end_local=excluded.end_local,updated_at=excluded.updated_at''',
          (interaction.guild_id,interaction.user.id,v.weekday,start,end,now_iso()))
        DB.commit()
        v.stop()
        await interaction.response.edit_message(content=(
            f'✅ Saved **{DAY_NAMES[v.weekday]}**: **{friendly_window(start,end)} Eastern**. '
            'This is your recurring availability, not a confirmed shift. '
            'Use `/availability` again to set another day.'),view=None)


class AvailabilityTimeView(discord.ui.View):
    def __init__(self,owner_id,weekday,start='09:00',end='17:00'):
        super().__init__(timeout=300)
        self.owner_id, self.weekday, self.start, self.end=owner_id,weekday,start,end
        self.add_item(AvailabilityTimeSelect(self,'start','hour',0))
        self.add_item(AvailabilityTimeSelect(self,'start','minute',1))
        self.add_item(AvailabilityTimeSelect(self,'end','hour',2))
        self.add_item(AvailabilityTimeSelect(self,'end','minute',3))
        self.add_item(AvailabilitySaveButton())

    def description(self):
        return (f'**{DAY_NAMES[self.weekday]} availability • Eastern Time**\n'
                f'Current selection: **{friendly_window(self.start,self.end)}**\n'
                'Choose start/end hours and minutes using the four dropdowns, then press **Save Availability**. '
                'Time options are in 15-minute increments. End time must be later than start time.')


class AvailabilityDaySelect(discord.ui.Select):
    def __init__(self):
        super().__init__(placeholder='Choose a weekday',options=[discord.SelectOption(label=n,value=str(i)) for i,n in enumerate(DAY_NAMES)])

    async def callback(self,interaction:discord.Interaction):
        if not allowed_guild(interaction) or not employee(interaction):
            await interaction.response.send_message('Employee access required.',ephemeral=True)
            return
        weekday=int(self.values[0])
        row=DB.execute('SELECT start_local,end_local FROM availability WHERE guild_id=? AND user_id=? AND weekday=?',
                       (interaction.guild_id,interaction.user.id,weekday)).fetchone()
        view=AvailabilityTimeView(interaction.user.id,weekday,row['start_local'] if row else '09:00',row['end_local'] if row else '17:00')
        await interaction.response.edit_message(content=view.description(),view=view)


class AvailabilityView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.add_item(AvailabilityDaySelect())


@bot.tree.command(name='availability',description='Employee: select your weekly availability with dropdown menus')
@app_commands.guild_only()
async def availability_command(interaction:discord.Interaction):
    if not allowed_guild(interaction) or not employee(interaction):
        await interaction.response.send_message('Employee access required.',ephemeral=True);return
    await interaction.response.send_message('**Set weekly availability** — choose a day below. Then select your start and end times from dropdown menus; no typing required. Repeat for additional days. Use `/clearavailability` to mark a day unavailable.',view=AvailabilityView(),ephemeral=True)


@bot.tree.command(name='myavailability',description='Employee: view your recurring availability')
@app_commands.guild_only()
async def myavailability(interaction:discord.Interaction):
    if not allowed_guild(interaction) or not employee(interaction):
        await interaction.response.send_message('Employee access required.',ephemeral=True);return
    rows=DB.execute('SELECT weekday,start_local,end_local FROM availability WHERE guild_id=? AND user_id=? ORDER BY weekday',(interaction.guild_id,interaction.user.id)).fetchall()
    daymap={r['weekday']:f'{friendly_window(r["start_local"],r["end_local"])}' for r in rows}
    body='\n'.join(f'**{day}:** {daymap.get(i,"Unavailable / not entered")}' for i,day in enumerate(DAY_NAMES))
    await interaction.response.send_message('**Your recurring weekly availability (Eastern):**\n'+body+'\n\nManagers must separately assign confirmed shifts.',ephemeral=True)


@bot.tree.command(name='clearavailability',description='Employee: mark a weekday as unavailable')
@app_commands.describe(day='Weekday to remove from your availability')
@app_commands.choices(day=[app_commands.Choice(name=n,value=i) for i,n in enumerate(DAY_NAMES)])
@app_commands.guild_only()
async def clearavailability(interaction:discord.Interaction,day:app_commands.Choice[int]):
    if not allowed_guild(interaction) or not employee(interaction):
        await interaction.response.send_message('Employee access required.',ephemeral=True);return
    DB.execute('DELETE FROM availability WHERE guild_id=? AND user_id=? AND weekday=?',(interaction.guild_id,interaction.user.id,day.value))
    DB.commit()
    await interaction.response.send_message(f'Updated: {day.name} is now unavailable/not entered.',ephemeral=True)


@bot.tree.command(name='availabilityreport',description='Manager: review employee availability')
@app_commands.guild_only()
async def availabilityreport(interaction:discord.Interaction,employee:discord.Member=None):
    if not allowed_guild(interaction) or not manager(interaction):
        await interaction.response.send_message('Manager permission required.',ephemeral=True);return
    sql='SELECT user_id,weekday,start_local,end_local FROM availability WHERE guild_id=?'
    params=[interaction.guild_id]
    if employee:
        sql+=' AND user_id=?';params.append(employee.id)
    sql+=' ORDER BY user_id,weekday'
    rows=DB.execute(sql,params).fetchall()
    await interaction.response.defer(ephemeral=True)
    await preload_employee_names(interaction.guild, [r['user_id'] for r in rows])
    lines=[f'{employee_name(interaction.guild,r["user_id"])} • {DAY_NAMES[r["weekday"]]}: {friendly_window(r["start_local"],r["end_local"])}' for r in rows]
    await interaction.followup.send('**Employee availability (Eastern):**\n'+('\n'.join(lines[:55]) or 'No availability submitted.')+('\n…more results; filter by employee.' if len(lines)>55 else ''),ephemeral=True,allowed_mentions=discord.AllowedMentions.none())


def assign_shift(guild_id,manager_id,employee_id,d,a,b):
    # Prevent duplicate overlapping assignments. Check weekly availability and warn in UI rather than block;
    # management can schedule with permission outside stated availability.
    overlaps=DB.execute('''SELECT id FROM planned_shifts WHERE guild_id=? AND user_id=? AND work_date=?
        AND cancelled_at IS NULL AND start_local<? AND end_local>?''',(guild_id,employee_id,d,b,a)).fetchone()
    if overlaps:
        raise ValueError(f'Overlaps existing assigned shift #{overlaps["id"]}. Cancel it first.')
    cursor=DB.execute('''INSERT INTO planned_shifts
        (guild_id,user_id,work_date,start_local,end_local,created_by,created_at)
        VALUES (?,?,?,?,?,?,?)''',(guild_id,employee_id,d,a,b,manager_id,now_iso()))
    shift_id=cursor.lastrowid
    DB.execute('INSERT INTO schedule_audit (guild_id,actor_id,action,detail,created_at) VALUES (?,?,?,?,?)',
        (guild_id,manager_id,'assign',f'#{shift_id} employee={employee_id}, {d} {a}-{b}',now_iso()))
    DB.commit()
    return shift_id


async def assign_and_notify(interaction,employee,d,a,b):
    weekday=date.fromisoformat(d).weekday()
    ava=DB.execute('SELECT * FROM availability WHERE guild_id=? AND user_id=? AND weekday=?',(interaction.guild_id,employee.id,weekday)).fetchone()
    outside=not ava or a<ava['start_local'] or b>ava['end_local']
    try:
        shift_id=assign_shift(interaction.guild_id,interaction.user.id,employee.id,d,a,b)
    except ValueError as exc:
        await interaction.followup.send(str(exc),ephemeral=True);return
    posted=await publish_week(interaction.guild,local_week_start(d).isoformat())
    try:
        await employee.send(f'📅 ETBeastDemo scheduled you for **{d} • {friendly_window(a,b)} Eastern** (shift #{shift_id}). Contact management if you have a conflict.')
    except (discord.Forbidden,discord.HTTPException):
        pass
    note=' ⚠️ This shift is outside their submitted availability or they have not entered availability.' if outside else ''
    note+=(' Weekly post updated.' if posted else ' Schedule saved; configure #schedule channel to post it.')
    await interaction.followup.send(f'✅ Assigned {employee.mention} to **{d} {friendly_window(a,b)} ET** (#{shift_id}).'+note,ephemeral=True,allowed_mentions=discord.AllowedMentions.none())


class AssignShiftModal(discord.ui.Modal,title='Assign a confirmed shift'):
    work_date=discord.ui.TextInput(label='Date (YYYY-MM-DD)',placeholder='2030-01-12',max_length=10)
    start=discord.ui.TextInput(label='Start (24-hour Eastern)',placeholder='09:00',max_length=5)
    end=discord.ui.TextInput(label='End (24-hour Eastern)',placeholder='17:00',max_length=5)
    def __init__(self,employee):
        super().__init__();self.employee=employee
    async def on_submit(self,interaction):
        if not manager(interaction) or not allowed_guild(interaction):
            await interaction.response.send_message('Manager permission required.',ephemeral=True);return
        try:
            d=date_from_text(self.work_date.value).isoformat()
            a,b=validate_window(self.start.value,self.end.value)
        except ValueError as exc:
            await interaction.response.send_message(str(exc),ephemeral=True);return
        await interaction.response.defer(ephemeral=True,thinking=True)
        await assign_and_notify(interaction,self.employee,d,a,b)


class ScheduleMemberSelect(discord.ui.UserSelect):
    def __init__(self,owner_id):
        super().__init__(placeholder='Select employee to schedule',min_values=1,max_values=1)
        self.owner_id=owner_id
    async def callback(self,interaction):
        if interaction.user.id != self.owner_id or not manager(interaction):
            await interaction.response.send_message('Manager permission required.',ephemeral=True);return
        member=self.values[0] if isinstance(self.values[0],discord.Member) else interaction.guild.get_member(self.values[0].id)
        if member is None:
            try:
                member=await interaction.guild.fetch_member(self.values[0].id)
            except (discord.NotFound,discord.Forbidden,discord.HTTPException):
                await interaction.response.send_message('Employee not found in server.',ephemeral=True);return
        await interaction.response.send_message('Select the scheduled date:',view=ManagerDateView(interaction.user.id,'assign',member=member),ephemeral=True)


class ScheduleMemberPicker(discord.ui.View):
    def __init__(self,owner_id):
        super().__init__(timeout=180)
        self.add_item(ScheduleMemberSelect(owner_id))


class ScheduleChannelSelect(discord.ui.ChannelSelect):
    def __init__(self,owner_id):
        super().__init__(placeholder='Choose your #schedule channel',channel_types=[discord.ChannelType.text]);self.owner_id=owner_id
    async def callback(self,interaction):
        if interaction.user.id!=self.owner_id or not manager(interaction):
            await interaction.response.send_message('Manager permission required.',ephemeral=True);return
        channel=self.values[0]
        DB.execute('INSERT INTO schedule_config VALUES (?,?) ON CONFLICT(guild_id) DO UPDATE SET channel_id=excluded.channel_id',(interaction.guild_id,channel.id))
        DB.execute('INSERT INTO schedule_audit (guild_id,actor_id,action,detail,created_at) VALUES (?,?,?,?,?)',(interaction.guild_id,interaction.user.id,'channel',f'{channel.id}',now_iso()))
        DB.commit()
        await interaction.response.defer(ephemeral=True)
        ok=await publish_week(interaction.guild,local_week_start().isoformat())
        await interaction.followup.send(('✅ Schedule channel set to '+channel.mention+'. Weekly schedule posted.' if ok else 'Saved channel, but I could not post. Check bot Send Messages/Embed Links permissions.'),ephemeral=True)


class ScheduleChannelPicker(discord.ui.View):
    def __init__(self,owner_id):
        super().__init__(timeout=180);self.add_item(ScheduleChannelSelect(owner_id))


# V17: manager-friendly edit/cancel for ASSIGNED shifts (not worked time entries).
# Never modify clock-in/out records from this scheduling workflow.
def active_planned_shift(guild_id, shift_id):
    return DB.execute('SELECT * FROM planned_shifts WHERE guild_id=? AND id=? AND cancelled_at IS NULL',
                      (guild_id, shift_id)).fetchone()


class ScheduledEmployeeSelect(discord.ui.UserSelect):
    def __init__(self, owner_id):
        super().__init__(placeholder='Choose employee whose schedule you want to change', min_values=1, max_values=1)
        self.owner_id=owner_id

    async def callback(self, interaction):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            return await interaction.response.send_message('Manager access required.',ephemeral=True)
        user_id=self.values[0].id
        picker=ScheduledShiftPicker(self.owner_id,interaction.guild_id,user_id)
        if not picker.has_rows:
            return await interaction.response.send_message('No upcoming assigned shifts found for that employee.',ephemeral=True)
        await interaction.response.send_message('Select an assigned shift:',view=picker,ephemeral=True)


class ScheduledEmployeePicker(discord.ui.View):
    def __init__(self,owner_id):
        super().__init__(timeout=300)
        self.add_item(ScheduledEmployeeSelect(owner_id))


class ScheduledShiftSelect(discord.ui.Select):
    def __init__(self,owner_id,user_id,rows):
        self.owner_id=owner_id
        opts=[discord.SelectOption(label=f'{r["work_date"]} • {friendly_window(r["start_local"],r["end_local"])}'[:100],
                                   value=str(r['id']),description=f'Shift #{r["id"]}') for r in rows]
        super().__init__(placeholder='Choose the scheduled shift',options=opts)

    async def callback(self,interaction):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            return await interaction.response.send_message('Manager access required.',ephemeral=True)
        shift_id=int(self.values[0]);row=active_planned_shift(interaction.guild_id,shift_id)
        if not row:
            return await interaction.response.send_message('This shift was already changed or cancelled. Reopen Scheduling.',ephemeral=True)
        await interaction.response.send_message('Choose what to do with this shift:',view=ScheduledShiftActions(self.owner_id,shift_id),ephemeral=True)


class ScheduledShiftPicker(discord.ui.View):
    def __init__(self,owner_id,guild_id,user_id):
        super().__init__(timeout=300)
        today=datetime.now(SCHEDULE_TZ).date().isoformat()
        rows=DB.execute('''SELECT * FROM planned_shifts WHERE guild_id=? AND user_id=?
             AND cancelled_at IS NULL AND work_date>=? ORDER BY work_date,start_local,id LIMIT 25''',
             (guild_id,user_id,today)).fetchall()
        if rows:self.add_item(ScheduledShiftSelect(owner_id,user_id,rows))
        self.has_rows=bool(rows)


class ScheduledShiftActions(discord.ui.View):
    def __init__(self,owner_id,shift_id):
        super().__init__(timeout=300);self.owner_id=owner_id;self.shift_id=shift_id
    async def interaction_check(self,interaction):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            await interaction.response.send_message('Manager access required.',ephemeral=True);return False
        return True
    @discord.ui.button(label='Edit Date / Time',emoji='✏️',style=discord.ButtonStyle.primary)
    async def edit(self,interaction,button):
        row=active_planned_shift(interaction.guild_id,self.shift_id)
        if not row:return await interaction.response.send_message('Shift no longer exists.',ephemeral=True)
        await interaction.response.send_message('Select a date and time:',view=PlannedShiftEditView(self.owner_id,self.shift_id,row),ephemeral=True)
    @discord.ui.button(label='Cancel Shift',emoji='🗑️',style=discord.ButtonStyle.danger)
    async def cancel(self,interaction,button):
        if not active_planned_shift(interaction.guild_id,self.shift_id):
            return await interaction.response.send_message('Shift no longer exists.',ephemeral=True)
        await interaction.response.send_modal(ScheduledCancelModal(self.shift_id))


class PlannedTimeSelect(discord.ui.Select):
    def __init__(self,parent,key,part,row):
        self.key=key;self.part=part
        current=parent.times[key].split(':')[0 if part=='hour' else 1]
        values=range(24) if part=='hour' else (0,15,30,45)
        opts=[discord.SelectOption(label=(f'{x%12 or 12} {"AM" if x<12 else "PM"}' if part=='hour' else f':{x:02d}'),
                                  value=f'{x:02d}',default=f'{x:02d}'==current) for x in values]
        super().__init__(placeholder=f'{key.title()} {part}',options=opts,row=row)
    async def callback(self,interaction):
        p=self.view
        if not await p.authorize(interaction):return
        pieces=p.times[self.key].split(':')
        pieces[0 if self.part=='hour' else 1]=self.values[0]
        p.times[self.key]=':'.join(pieces)
        await interaction.response.edit_message(content=p.summary(),view=PlannedShiftTimeView(p.owner_id,p.shift_id,p.day,p.times.copy(),p.page))


class PlannedDaySelect(discord.ui.Select):
    def __init__(self,parent):
        today=datetime.now(SCHEDULE_TZ).date()
        dates=[today+timedelta(days=x) for x in range(parent.page*25,min(parent.page*25+25,121))]
        opts=[discord.SelectOption(label=d.strftime('%a, %b %d, %Y'),value=d.isoformat(),default=d==parent.day) for d in dates]
        super().__init__(placeholder='Scheduled date',options=opts,row=0)
    async def callback(self,interaction):
        p=self.view
        if not await p.authorize(interaction):return
        await interaction.response.edit_message(content=p.summary(day=date.fromisoformat(self.values[0])),
            view=PlannedShiftTimeView(p.owner_id,p.shift_id,date.fromisoformat(self.values[0]),p.times.copy(),p.page))


class PlannedShiftEditView(discord.ui.View):
    def __init__(self,owner_id,shift_id,shift=None,day=None,times=None,page=0):
        super().__init__(timeout=600)
        self.owner_id=owner_id;self.shift_id=shift_id;self.page=page
        self.day=day or date.fromisoformat(shift['work_date'])
        self.times=times or {'start':shift['start_local'],'end':shift['end_local']}
        self.add_item(PlannedDaySelect(self))

    async def authorize(self,interaction):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            await interaction.response.send_message('Manager access required.',ephemeral=True);return False
        return True
    def summary(self,day=None):
        return f'**Edit scheduled shift #{self.shift_id}**\nDate: {(day or self.day):%b %d, %Y}\nTime: {friendly_window(self.times["start"],self.times["end"])} Eastern\nChoose the date and times, then review.'
    @discord.ui.button(label='Previous Dates',style=discord.ButtonStyle.secondary,row=1)
    async def previous(self,interaction,button):
        if not await self.authorize(interaction):return
        pg=max(0,self.page-1)
        await interaction.response.edit_message(content=self.summary(),view=PlannedShiftEditView(self.owner_id,self.shift_id,None,self.day,self.times.copy(),pg))
    @discord.ui.button(label='Next Dates',style=discord.ButtonStyle.secondary,row=1)
    async def next(self,interaction,button):
        if not await self.authorize(interaction):return
        pg=min(4,self.page+1)
        await interaction.response.edit_message(content=self.summary(),view=PlannedShiftEditView(self.owner_id,self.shift_id,None,self.day,self.times.copy(),pg))
    @discord.ui.button(label='Choose Times',style=discord.ButtonStyle.success,row=1)
    async def choose_times(self,interaction,button):
        if not await self.authorize(interaction):return
        await interaction.response.edit_message(content=self.summary(),view=PlannedShiftTimeView(self.owner_id,self.shift_id,self.day,self.times.copy(),self.page))


class PlannedShiftTimeView(discord.ui.View):
    def __init__(self,owner_id,shift_id,day,times,page):
        super().__init__(timeout=600)
        self.owner_id=owner_id;self.shift_id=shift_id;self.day=day;self.times=times;self.page=page
        for row,(key,part) in enumerate([('start','hour'),('start','minute'),('end','hour'),('end','minute')]):
            self.add_item(PlannedTimeSelect(self,key,part,row))
    async def authorize(self,interaction):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            await interaction.response.send_message('Manager access required.',ephemeral=True);return False
        return True
    def summary(self):
        return f'**Edit scheduled shift #{self.shift_id}**\nDate: {self.day:%b %d, %Y}\nTime: {friendly_window(self.times["start"],self.times["end"])} Eastern'
    @discord.ui.button(label='Change Date',style=discord.ButtonStyle.secondary,row=4)
    async def change_date(self,interaction,button):
        if not await self.authorize(interaction):return
        await interaction.response.edit_message(content=self.summary(),view=PlannedShiftEditView(self.owner_id,self.shift_id,None,self.day,self.times.copy(),self.page))
    @discord.ui.button(label='Review Change',style=discord.ButtonStyle.success,row=4)
    async def review(self,interaction,button):
        if not await self.authorize(interaction):return
        row=active_planned_shift(interaction.guild_id,self.shift_id)
        if not row:return await interaction.response.send_message('Shift was already cancelled.',ephemeral=True)
        try:
            d=date_from_text(self.day.isoformat()).isoformat()
            a,b=validate_window(self.times['start'],self.times['end'])
            overlapping=DB.execute('''SELECT id FROM planned_shifts WHERE guild_id=? AND user_id=?
                AND work_date=? AND cancelled_at IS NULL AND id<>? AND start_local<? AND end_local>?''',
                (interaction.guild_id,row['user_id'],d,self.shift_id,b,a)).fetchone()
            if overlapping:raise ValueError(f'Overlaps another shift #{overlapping["id"]}.')
        except ValueError as exc:
            return await interaction.response.send_message(f'⚠️ {exc}',ephemeral=True)
        await interaction.response.send_message(
            f'**Confirm schedule edit**\nOriginal: {row["work_date"]} {friendly_window(row["start_local"],row["end_local"])}\nNew: {d} {friendly_window(a,b)}\nThis will update #schedule and notify the employee.',
            view=PlannedEditConfirm(self.owner_id,self.shift_id,(row['work_date'],row['start_local'],row['end_local']),d,a,b),ephemeral=True)


class PlannedEditConfirm(discord.ui.View):
    def __init__(self,owner_id,shift_id,original,d,a,b):
        super().__init__(timeout=180);self.owner_id=owner_id;self.shift_id=shift_id
        self.original=original;self.d=d;self.a=a;self.b=b
    @discord.ui.button(label='Confirm Edit',style=discord.ButtonStyle.success)
    async def confirm(self,interaction,button):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            return await interaction.response.send_message('Manager access required.',ephemeral=True)
        row=active_planned_shift(interaction.guild_id,self.shift_id)
        if not row or (row['work_date'],row['start_local'],row['end_local'])!=self.original:
            return await interaction.response.send_message('Shift has changed since you opened this menu. Reopen it.',ephemeral=True)
        try:
            d=date_from_text(self.d).isoformat();a,b=validate_window(self.a,self.b)
            overlap=DB.execute('''SELECT id FROM planned_shifts WHERE guild_id=? AND user_id=? AND work_date=?
                AND cancelled_at IS NULL AND id<>? AND start_local<? AND end_local>?''',
                (interaction.guild_id,row['user_id'],d,self.shift_id,b,a)).fetchone()
            if overlap:raise ValueError(f'Overlaps shift #{overlap["id"]}.')
        except ValueError as exc:
            return await interaction.response.send_message(str(exc),ephemeral=True)
        DB.execute('UPDATE planned_shifts SET work_date=?,start_local=?,end_local=? WHERE id=? AND guild_id=?',
                   (d,a,b,self.shift_id,interaction.guild_id))
        DB.execute('INSERT INTO schedule_audit (guild_id,actor_id,action,detail,created_at) VALUES (?,?,?,?,?)',
                   (interaction.guild_id,interaction.user.id,'edit',f'#{self.shift_id}: {self.original} -> {(d,a,b)}',now_iso()))
        DB.commit();self.stop()
        await interaction.response.defer(ephemeral=True)
        old_week=local_week_start(self.original[0]).isoformat();new_week=local_week_start(d).isoformat()
        p1=await publish_week(interaction.guild,old_week)
        p2=await publish_week(interaction.guild,new_week) if new_week!=old_week else p1
        try:
            member=interaction.guild.get_member(row['user_id']) or await interaction.guild.fetch_member(row['user_id'])
            await member.send(f'📅 ETBeastDemo: your scheduled shift has changed to **{d} • {friendly_window(a,b)} Eastern**. Contact management with any conflicts.')
        except (discord.NotFound,discord.Forbidden,discord.HTTPException):pass
        await interaction.followup.send(f'✅ Shift #{self.shift_id} updated. '+('Schedule post updated.' if p1 and p2 else 'Schedule saved; check #schedule bot permissions.'),ephemeral=True)


class ScheduledCancelModal(discord.ui.Modal,title='Cancel scheduled shift'):
    reason=discord.ui.TextInput(label='Reason for cancellation',min_length=3,max_length=250)
    def __init__(self,shift_id):super().__init__();self.shift_id=shift_id
    async def on_submit(self,interaction):
        if not manager(interaction) or not allowed_guild(interaction):
            return await interaction.response.send_message('Manager access required.',ephemeral=True)
        row=active_planned_shift(interaction.guild_id,self.shift_id)
        if not row:return await interaction.response.send_message('Shift already cancelled.',ephemeral=True)
        await interaction.response.send_message(
            f'**Confirm cancellation**\n{employee_name(interaction.guild,row["user_id"])} — {row["work_date"]}, {friendly_window(row["start_local"],row["end_local"])}\nReason: {self.reason.value}',
            view=ScheduledCancelConfirm(interaction.user.id,self.shift_id,tuple(row[k] for k in ('work_date','start_local','end_local')),str(self.reason.value)),ephemeral=True)


class ScheduledCancelConfirm(discord.ui.View):
    def __init__(self,owner_id,shift_id,original,reason):
        super().__init__(timeout=180);self.owner_id=owner_id;self.shift_id=shift_id;self.original=original;self.reason=reason
    @discord.ui.button(label='Confirm Cancellation',style=discord.ButtonStyle.danger)
    async def confirm(self,interaction,button):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            return await interaction.response.send_message('Manager access required.',ephemeral=True)
        row=active_planned_shift(interaction.guild_id,self.shift_id)
        if not row or tuple(row[k] for k in ('work_date','start_local','end_local'))!=self.original:
            return await interaction.response.send_message('Shift changed. Reopen Scheduling and try again.',ephemeral=True)
        DB.execute('UPDATE planned_shifts SET cancelled_at=?,cancelled_by=?,cancellation_reason=? WHERE id=? AND guild_id=? AND cancelled_at IS NULL',
                   (now_iso(),interaction.user.id,self.reason,self.shift_id,interaction.guild_id))
        DB.execute('INSERT INTO schedule_audit (guild_id,actor_id,action,detail,created_at) VALUES (?,?,?,?,?)',
                   (interaction.guild_id,interaction.user.id,'cancel',f'#{self.shift_id}: {self.reason}',now_iso()))
        DB.commit();self.stop()
        await interaction.response.defer(ephemeral=True)
        posted=await publish_week(interaction.guild,local_week_start(row['work_date']).isoformat())
        try:
            member=interaction.guild.get_member(row['user_id']) or await interaction.guild.fetch_member(row['user_id'])
            await member.send(f'ETBeastDemo: your shift on {row["work_date"]} at {friendly_window(row["start_local"],row["end_local"])} was cancelled. Reason: {self.reason}')
        except (discord.NotFound,discord.Forbidden,discord.HTTPException):pass
        await interaction.followup.send(f'✅ Shift #{self.shift_id} cancelled. '+('Weekly schedule updated.' if posted else 'Check schedule channel permissions.'),ephemeral=True)


class SchedulingManagerView(discord.ui.View):
    def __init__(self,owner_id):
        super().__init__(timeout=300);self.owner_id=owner_id
    async def interaction_check(self,interaction):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            await interaction.response.send_message('Manager permission required.',ephemeral=True);return False
        return True
    @discord.ui.button(label='Assign Shift',emoji='➕',style=discord.ButtonStyle.success)
    async def assign_button(self,interaction,button):
        await interaction.response.send_message('Choose who to schedule:',view=ScheduleMemberPicker(interaction.user.id),ephemeral=True)
    @discord.ui.button(label='Edit / Cancel Shift',emoji='✏️',style=discord.ButtonStyle.primary)
    async def edit_cancel_button(self,interaction,button):
        await interaction.response.send_message('Select the employee whose scheduled shift you want to edit or cancel:',view=ScheduledEmployeePicker(self.owner_id),ephemeral=True)
    @discord.ui.button(label='Employee Availability',emoji='🕒',style=discord.ButtonStyle.secondary)
    async def availability_button(self,interaction,button):
        await availabilityreport.callback(interaction,employee=None)
    @discord.ui.button(label='Set Schedule Channel',emoji='📢',style=discord.ButtonStyle.secondary)
    async def channel_button(self,interaction,button):
        await interaction.response.send_message('Choose #schedule:',view=ScheduleChannelPicker(interaction.user.id),ephemeral=True)
    @discord.ui.button(label='Post Weekly Schedule',emoji='📅',style=discord.ButtonStyle.primary)
    async def post_button(self,interaction,button):
        await interaction.response.defer(ephemeral=True)
        ok=await publish_week(interaction.guild,local_week_start().isoformat())
        await interaction.followup.send('✅ Weekly schedule posted/updated.' if ok else 'Unable to post. Set the schedule channel and check permissions.',ephemeral=True)


@bot.tree.command(name='schedulemanager',description='Manager: open the scheduling panel')
@app_commands.guild_only()
async def schedulemanager(interaction:discord.Interaction):
    if not manager(interaction) or not allowed_guild(interaction):
        await interaction.response.send_message('Manager permission required.',ephemeral=True);return
    await interaction.response.send_message('**ETBeastDemo Scheduling** — use the buttons to assign shifts, review availability, or choose #schedule.',view=SchedulingManagerView(interaction.user.id),ephemeral=True)


@bot.tree.command(name='assignshift',description='Manager: assign a dated shift to an employee')
@app_commands.guild_only()
async def assignshift(interaction:discord.Interaction,employee:discord.Member,work_date:str,start:str,end:str):
    if not manager(interaction) or not allowed_guild(interaction):
        await interaction.response.send_message('Manager permission required.',ephemeral=True);return
    try:
        d=date_from_text(work_date).isoformat();a,b=validate_window(start,end)
    except ValueError as exc:
        await interaction.response.send_message(str(exc),ephemeral=True);return
    await interaction.response.defer(ephemeral=True,thinking=True)
    await assign_and_notify(interaction,employee,d,a,b)


@bot.tree.command(name='unassignshift',description='Manager: cancel a scheduled shift, keeping history')
@app_commands.guild_only()
async def unassignshift(interaction:discord.Interaction,shift_id:int,reason:str):
    if not manager(interaction) or not allowed_guild(interaction):
        await interaction.response.send_message('Manager permission required.',ephemeral=True);return
    if len(reason.strip())<3:
        await interaction.response.send_message('Provide a short reason.',ephemeral=True);return
    row=DB.execute('SELECT * FROM planned_shifts WHERE guild_id=? AND id=? AND cancelled_at IS NULL',(interaction.guild_id,shift_id)).fetchone()
    if not row:
        await interaction.response.send_message('Active scheduled shift not found.',ephemeral=True);return
    DB.execute('UPDATE planned_shifts SET cancelled_at=?,cancelled_by=?,cancellation_reason=? WHERE id=?',(now_iso(),interaction.user.id,reason,shift_id))
    DB.execute('INSERT INTO schedule_audit (guild_id,actor_id,action,detail,created_at) VALUES (?,?,?,?,?)',(interaction.guild_id,interaction.user.id,'cancel',f'#{shift_id}: {reason}',now_iso()))
    DB.commit()
    await interaction.response.defer(ephemeral=True)
    await publish_week(interaction.guild,local_week_start(row['work_date']).isoformat())
    await interaction.followup.send(f'Cancelled schedule shift #{shift_id}. This did **not** edit recorded clock-in hours.',ephemeral=True)
    try:
        member=interaction.guild.get_member(row['user_id'])
        if member:
            await member.send(f'ETBeastDemo: your scheduled shift #{shift_id} on {row["work_date"]} was cancelled. Ask management if you have questions.')
    except (discord.Forbidden,discord.HTTPException):
        pass


@bot.tree.command(name='schedule',description='View confirmed shifts for the current week')
@app_commands.guild_only()
async def schedule_command(interaction:discord.Interaction):
    await interaction.response.send_message(embed=schedule_embed(interaction.guild,local_week_start().isoformat()),ephemeral=True,allowed_mentions=discord.AllowedMentions.none())


@bot.tree.command(name='postschedule',description='Manager: refresh the confirmed weekly schedule post')
@app_commands.guild_only()
async def postschedule(interaction:discord.Interaction):
    if not manager(interaction) or not allowed_guild(interaction):
        await interaction.response.send_message('Manager permission required.',ephemeral=True);return
    await interaction.response.defer(ephemeral=True)
    ok=await publish_week(interaction.guild,local_week_start().isoformat())
    await interaction.followup.send('✅ Weekly schedule updated.' if ok else 'Unable to post. Set #schedule using /schedulemanager and check bot permissions.',ephemeral=True)



# V10: role-aware home screens. Every action rechecks identity and role server-side.
# UI views are ephemeral and expire after inactivity. Slash commands remain usable.
def employee_home_embed(interaction):
    open_shift = DB.execute('SELECT id,clock_in,work_type FROM shifts WHERE guild_id=? AND user_id=? AND clock_out IS NULL',
                            (interaction.guild_id,interaction.user.id)).fetchone()
    pending = DB.execute("SELECT count(*) AS n FROM commissions WHERE guild_id=? AND user_id=? AND status='pending'",
                         (interaction.guild_id,interaction.user.id)).fetchone()['n']
    start = open_shift['clock_in'] if open_shift else None
    status = (f'🟢 On shift since <t:{int(datetime.fromisoformat(start).timestamp())}:t> (shift #{open_shift["id"]})'
              if start else '⚪ Not currently clocked in')
    e = discord.Embed(title='ETBeastDemo • Employee Dashboard',
                      description='Your private workspace. Tap a button to manage your shift or availability.',
                      color=discord.Color.green())
    e.add_field(name='Time clock',value=status+(f' • {open_shift["work_type"].title()}' if open_shift else ''),inline=False)
    e.add_field(name='Commission submissions awaiting approval',value=str(pending))
    e.set_footer(text='Streaming: employee rate + commission • Packing: employee rate • Example segment minimum • Eastern Time')
    return e


def employee_pay_embed(interaction):
    # Summary only: does not calculate overtime, taxes, or net pay.
    cutoff=(datetime.now(timezone.utc)-timedelta(days=7)).isoformat()
    rows=DB.execute('SELECT clock_in,clock_out,hourly_rate_cents FROM shifts WHERE guild_id=? AND user_id=? AND clock_out>=?',
                    (interaction.guild_id,interaction.user.id,cutoff)).fetchall()
    hours=0.0; wages=0
    for r in rows:
        h=max(0,(datetime.fromisoformat(r['clock_out'])-datetime.fromisoformat(r['clock_in'])).total_seconds()/3600)
        hours+=h
        wages+=int((Decimal(str(h))*int(r['hourly_rate_cents'] or rates_for(interaction.guild_id,interaction.user.id)[0])).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
    commission=DB.execute("SELECT COALESCE(SUM(commission_cents),0) AS n, COALESCE(SUM(guarantee_topup_cents),0) AS topup FROM commissions WHERE guild_id=? AND user_id=? AND status='approved' AND submitted_at>=?",
                          (interaction.guild_id,interaction.user.id,cutoff)).fetchone()
    bonuses=DB.execute('SELECT COALESCE(SUM(amount_cents),0) AS n FROM bonuses WHERE guild_id=? AND employee_id=? AND voided_at IS NULL AND created_at>=?',
                       (interaction.guild_id,interaction.user.id,cutoff)).fetchone()['n']
    e=discord.Embed(title='Your earnings • last 7 rolling days',description='Estimates only; not a payslip or final payroll.',color=discord.Color.blurple())
    e.add_field(name='Completed hours',value=f'{hours:.2f} hr')
    e.add_field(name='Base hourly wages',value=money(wages))
    e.add_field(name='Approved commissions',value=money(commission['n']))
    e.add_field(name='Character Break guarantee top-ups',value=money(commission['topup']))
    e.add_field(name='Bonuses',value=money(bonuses))
    e.add_field(name='Estimated total',value=money(wages+commission['n']+commission['topup']+bonuses),inline=False)
    e.set_footer(text='Excludes overtime premiums, taxes and adjustments; periods are rolling and components may have differing dates.')
    return e


# V32: private weekly statements. Values are derived from the same ledger as management.
def employee_statement_embed(guild_id, user_id, week_start):
    live = payroll_week_data(guild_id,week_start)
    week = payroll_display_week(guild_id,week_start,live)
    status = payroll_week_status(guild_id,week_start)
    row = week['employees'].get(str(user_id),{})
    payments = employee_payment_totals(guild_id,week_start).get(str(user_id),0)
    base=int(row.get('base',0));commission=int(row.get('commission',0))
    topup=int(row.get('topup',0));bonus=int(row.get('bonus',0))
    gross=base+commission+topup+bonus
    fine_deductions=int(row.get('fine_deductions',0))
    payable=max(0,gross-fine_deductions)
    label = status['status'].upper() if status else 'DRAFT'
    if status and status['fingerprint'] != live['fingerprint']:
        label += ' · LIVE RECORDS DIFFER; APPROVED VALUES SHOWN'
    if week.get('reconciliation_adjusted'):
        label += ' · DUPLICATE LEGACY COMMISSIONS RECONCILED'
    embed=discord.Embed(title=f'💵 Your Pay Statement · {week["start"]} – {week["end"]}',
                        description=f'**{label}** · Eastern Time · Private to you',
                        color=discord.Color.blurple())
    embed.add_field(name='Streaming hours',value=f'{Decimal(row.get("stream_hours","0")):.2f} h')
    embed.add_field(name='Packing hours',value=f'{Decimal(row.get("packing_hours","0")):.2f} h')
    embed.add_field(name='Total hours',value=f'{Decimal(row.get("hours","0")):.2f} h')
    embed.add_field(name='Included manual weekly hours',value=f'{Decimal(row.get("manual_hours","0")):.2f} h · {money(int(row.get("manual_base",0)))} wages')
    embed.add_field(name='Base wages',value=money(base))
    embed.add_field(name='Approved commissions',value=money(commission))
    embed.add_field(name='Character Break top-ups',value=money(topup))
    embed.add_field(name='Bonuses',value=money(bonus))
    embed.add_field(name='Gross earnings',value=f'**{money(gross)}**')
    embed.add_field(name='Authorized fines (deductions)',value=f'−{money(fine_deductions)}')
    fine_rows=DB.execute('''SELECT d.incident_id,d.amount_cents,f.reason
        FROM fine_payroll_deductions d JOIN incident_fines f ON f.id=d.incident_id
        WHERE d.guild_id=? AND d.week_start=? AND d.employee_id=?
        ORDER BY d.id DESC''',(guild_id,week['start'],user_id)).fetchall()
    if fine_rows:
        details='\n'.join(f'#{r["incident_id"]}: −{money(r["amount_cents"])} · {discord.utils.escape_mentions(r["reason"][:70])}' for r in fine_rows[:10])
        embed.add_field(name='Fine details',value=details[:1024],inline=False)
    embed.add_field(name='Payable after deductions',value=f'**{money(payable)}**')
    embed.add_field(name='Payments already sent',value=money(payments))
    embed.add_field(name='Remaining balance',value=f'**{money(payable-payments)}**',inline=False)
    embed.set_footer(text='Approved weeks show saved approval amounts. Remaining balance is based on transfers recorded in ETBeast; verify externally paid amounts before another payment.')
    return embed


class EmployeeStatementView(discord.ui.View):
    def __init__(self, user_id, guild_id, week_start):
        super().__init__(timeout=300)
        self.user_id=user_id;self.guild_id=guild_id
        sunday=payroll_week_start(datetime.now(PAYROLL_TZ))
        dates=[sunday-timedelta(weeks=i) for i in range(16)]
        options=[discord.SelectOption(label=f'{d:%b %d} – {(d+timedelta(days=6)):%b %d, %Y}',
                 value=d.isoformat(),default=d.isoformat()==week_start) for d in dates]
        dropdown=discord.ui.Select(placeholder='Select a Sunday–Saturday pay period',options=options)
        async def choose(interaction):
            value=dropdown.values[0]
            await interaction.response.edit_message(embed=employee_statement_embed(self.guild_id,self.user_id,value),
                                                   view=EmployeeStatementView(self.user_id,self.guild_id,value))
        dropdown.callback=choose
        self.add_item(dropdown)

    async def interaction_check(self,interaction):
        if interaction.user.id != self.user_id or interaction.guild_id != self.guild_id or not allowed_guild(interaction):
            await interaction.response.send_message('This private statement belongs to another employee.',ephemeral=True)
            return False
        return True


@bot.tree.command(name='mystatement',description='Privately view your exact weekly earnings and payments')
@app_commands.guild_only()
async def mystatement_command(interaction:discord.Interaction):
    if not allowed_guild(interaction) or not employee(interaction):
        return await interaction.response.send_message('Employee access required.',ephemeral=True)
    start=payroll_week_start(datetime.now(PAYROLL_TZ)).isoformat()
    await interaction.response.send_message(embed=employee_statement_embed(interaction.guild_id,interaction.user.id,start),
        view=EmployeeStatementView(interaction.user.id,interaction.guild_id,start),ephemeral=True)


class EmployeeHome(discord.ui.View):
    def __init__(self,owner_id):
        super().__init__(timeout=600)
        self.owner_id=owner_id

    async def interaction_check(self,interaction):
        if (interaction.user.id!=self.owner_id or not allowed_guild(interaction) or not employee(interaction)):
            await interaction.response.send_message('Open your own /dashboard to use this menu.',ephemeral=True)
            return False
        return True

    @discord.ui.button(label='My Weekly Statement',emoji='💵',style=discord.ButtonStyle.success,row=0)
    async def statement_button(self,interaction,button):
        start=payroll_week_start(datetime.now(PAYROLL_TZ)).isoformat()
        await interaction.response.send_message(embed=employee_statement_embed(interaction.guild_id,interaction.user.id,start),
           view=EmployeeStatementView(interaction.user.id,interaction.guild_id,start),ephemeral=True)

    @discord.ui.button(label='Clock In',emoji='🟢',style=discord.ButtonStyle.success,row=0)
    async def clockin_button(self,interaction,button):
        await interaction.response.send_message('Choose Streaming or Packing:',view=WorkTypeChooser(interaction.user.id),ephemeral=True)

    @discord.ui.button(label='Clock Out',emoji='🔴',style=discord.ButtonStyle.danger,row=0)
    async def clockout_button(self,interaction,button):
        await clock_out(interaction)

    @discord.ui.button(label='Start Break',emoji='🍽️',style=discord.ButtonStyle.secondary,row=1)
    async def startbreak_button(self,interaction,button):
        await start_break(interaction)

    @discord.ui.button(label='End Break / Resume',emoji='▶️',style=discord.ButtonStyle.secondary,row=1)
    async def endbreak_button(self,interaction,button):
        await end_break(interaction)

    @discord.ui.button(label='Switch Work Type',emoji='🔁',style=discord.ButtonStyle.primary,row=2)
    async def switch_button(self,interaction,button):
        await interaction.response.send_message('Choose your new work type:',view=WorkSwitchChooser(interaction.user.id),ephemeral=True)

    @discord.ui.button(label='Submit Revenue',emoji='📷',style=discord.ButtonStyle.primary,row=2)
    async def revenue_button(self,interaction,button):
        await interaction.response.send_message(
            '**Submit revenue**\n'
            'Use **`/commission`** and select **Surprise Set** or **Character Break**.\n'
            'For Character Breaks, select **20 / 50 / 100 pack** and the **number of breaks** shown. No sales/hour needed.\n'
            'Enter combined revenue and attach 1–5 screenshots. For Character Breaks, select pack size and number of breaks.\n'
            'No shift ID, order count, or duration needed. Your current/latest Streaming shift is used. '
            'Stay clocked in while switching formats. Management verifies the screenshot.',
            ephemeral=True)

    @discord.ui.button(label='Set Availability',emoji='🗓️',style=discord.ButtonStyle.primary,row=1)
    async def availability_button(self,interaction,button):
        await availability_command.callback(interaction)

    @discord.ui.button(label='My Availability',emoji='🕒',style=discord.ButtonStyle.secondary,row=1)
    async def myavailability_button(self,interaction,button):
        await myavailability.callback(interaction)

    @discord.ui.button(label='View Schedule',emoji='📅',style=discord.ButtonStyle.secondary,row=1)
    async def schedule_button(self,interaction,button):
        await schedule_command.callback(interaction)

    @discord.ui.button(label='Mark Day Unavailable',emoji='🚫',style=discord.ButtonStyle.secondary,row=2)
    async def clear_button(self,interaction,button):
        await interaction.response.send_message('Select a day to remove your availability:',view=EmployeeClearAvailability(self.owner_id),ephemeral=True)

    @discord.ui.button(label='Refresh',emoji='🔄',style=discord.ButtonStyle.secondary,row=2)
    async def refresh_button(self,interaction,button):
        await interaction.response.edit_message(embed=employee_home_embed(interaction),view=self)


class EmployeeClearAvailability(discord.ui.View):
    def __init__(self,owner_id):
        super().__init__(timeout=180)
        self.owner_id=owner_id
        select=discord.ui.Select(placeholder='Choose day to mark unavailable',options=[discord.SelectOption(label=d,value=str(i)) for i,d in enumerate(DAY_NAMES)])
        async def chosen(interaction):
            if interaction.user.id!=self.owner_id or not employee(interaction) or not allowed_guild(interaction):
                await interaction.response.send_message('Access denied.',ephemeral=True);return
            day=int(select.values[0])
            async with lock:
                DB.execute('DELETE FROM availability WHERE guild_id=? AND user_id=? AND weekday=?',(interaction.guild_id,interaction.user.id,day))
                DB.commit()
            await interaction.response.send_message(f'✅ {DAY_NAMES[day]} marked unavailable. Existing confirmed shifts are not cancelled; contact management if needed.',ephemeral=True)
        select.callback=chosen
        self.add_item(select)


class ManagerHome(discord.ui.View):
    def __init__(self,owner_id):
        super().__init__(timeout=600); self.owner_id=owner_id

    async def interaction_check(self,interaction):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            await interaction.response.send_message('Manager access required. Open your own /dashboard.',ephemeral=True)
            return False
        return True

    @discord.ui.button(label='Scheduling',emoji='📅',style=discord.ButtonStyle.primary,row=0)
    async def schedule_button(self,interaction,button):
        await interaction.response.send_message('**Scheduling** — review availability, assign shifts, or set #schedule.',view=SchedulingManagerView(self.owner_id),ephemeral=True)

    @discord.ui.button(label='Time & Shifts',emoji='⏱️',style=discord.ButtonStyle.primary,row=0)
    async def time_button(self,interaction,button):
        await interaction.response.send_message('**Time & Shifts**',view=ManagerSection(self.owner_id,'time'),ephemeral=True)

    @discord.ui.button(label='Revenue & Performance',emoji='📈',style=discord.ButtonStyle.primary,row=0)
    async def revenue_button(self,interaction,button):
        await interaction.response.send_message('**Revenue & Performance**',view=ManagerSection(self.owner_id,'revenue'),ephemeral=True)

    @discord.ui.button(label='Payroll & Bonuses',emoji='💵',style=discord.ButtonStyle.success,row=1)
    async def pay_button(self,interaction,button):
        await interaction.response.send_message('**Payroll & Bonuses**',view=ManagerSection(self.owner_id,'pay'),ephemeral=True)

    @discord.ui.button(label='Settings & Records',emoji='⚙️',style=discord.ButtonStyle.secondary,row=1)
    async def admin_button(self,interaction,button):
        await interaction.response.send_message('**Settings & Records**',view=ManagerSection(self.owner_id,'admin'),ephemeral=True)

    @discord.ui.button(label='Employee Tools',emoji='👤',style=discord.ButtonStyle.secondary,row=1)
    async def employee_button(self,interaction,button):
        await interaction.response.send_message(embed=employee_home_embed(interaction),view=EmployeeHome(self.owner_id),ephemeral=True)

    @discord.ui.button(label='Refresh',emoji='🔄',style=discord.ButtonStyle.secondary,row=2)
    async def refresh_button(self,interaction,button):
        await interaction.response.edit_message(embed=boss_embed(interaction.guild_id),view=self)


class V38ReasonModal(discord.ui.Modal,title='Confirm manager shift action'):
    reason=discord.ui.TextInput(label='Reason (required)',min_length=5,max_length=300)
    # Blank means current time, not a fabricated earlier time.
    end_time=discord.ui.TextInput(label='Exact clock-out (optional, ISO with offset)',
        required=False,max_length=40,placeholder='2030-01-12T17:43:00-04:00')
    def __init__(self,kind,owner_id,shift_id,user_id):
        super().__init__();self.kind=kind;self.owner_id=owner_id;self.shift_id=shift_id;self.user_id=user_id
        if kind=='void':self.remove_item(self.end_time)
    async def on_submit(self,interaction):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            return await interaction.response.send_message('Manager permission required.',ephemeral=True)
        try:
            async with lock:
                if self.kind=='void':
                    row=void_clocked_shift_record(interaction.guild_id,self.shift_id,interaction.user.id,str(self.reason))
                else:
                    row,finish=force_clock_out_record(interaction.guild_id,self.user_id,interaction.user.id,
                                                      str(self.reason),str(self.end_time).strip() or None)
        except (ValueError,sqlite3.IntegrityError) as exc:
            return await interaction.response.send_message(f'❌ {exc}',ephemeral=True)
        if self.kind=='void':
            await interaction.response.send_message(f'✅ Voided shift #{self.shift_id}. Original record archived; removed from payroll totals.',ephemeral=True)
            await notify_shift_correction(interaction,f'🗑️ Shift #{self.shift_id} voided by {interaction.user.display_name}; reason: {self.reason}')
        else:
            await interaction.response.send_message(f'✅ Force clock-out: shift #{self.shift_id} closed at {finish}.',ephemeral=True)
            event_id=enqueue_clock_log(interaction.guild_id,self.user_id,
                await resolve_employee_name(interaction.guild,self.user_id),'out',self.shift_id,
                row['clock_in'],finish,row['hourly_rate_cents'],event_type='manager_force_out')
            if event_id is not None:await deliver_employee_log(event_id)
            await notify_shift_correction(interaction,f'🔒 Shift #{self.shift_id} force-clocked out by {interaction.user.display_name} at {finish}; reason: {self.reason}')


class V38ShiftSelect(discord.ui.Select):
    def __init__(self,owner_id,rows,kind):
        self.owner_id=owner_id;self.kind=kind
        super().__init__(placeholder='Choose shift',options=[discord.SelectOption(
            label=f'#{r["id"]} · employee {r["user_id"]} · {boss_local(r["clock_in"])}'[:100],value=str(r['id']))
            for r in rows])
    async def callback(self,interaction):
        if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
            return await interaction.response.send_message('Manager permission required.',ephemeral=True)
        shift=DB.execute('SELECT * FROM shifts WHERE guild_id=? AND id=?',(interaction.guild_id,int(self.values[0]))).fetchone()
        if not shift:return await interaction.response.send_message('Shift not found.',ephemeral=True)
        await interaction.response.send_modal(V38ReasonModal(self.kind,self.owner_id,shift['id'],shift['user_id']))


class V38ShiftPicker(discord.ui.View):
    def __init__(self,owner_id,rows):
        super().__init__(timeout=180);self.add_item(V38ShiftSelect(owner_id,rows,'void'))


class V38ForcePicker(discord.ui.View):
    def __init__(self,owner_id,rows):
        super().__init__(timeout=180);self.add_item(V38ShiftSelect(owner_id,rows,'force'))


class ManagerSection(discord.ui.View):
    def __init__(self,owner_id,section):
        super().__init__(timeout=300); self.owner_id=owner_id; self.section=section
        groups={
          'time': [('Weekly Hours','hours'),('Fix a Shift','fixshift'),('Add Missing Shift','addshift'),('Manual Weekly Hours','manualweekly'),('Void Clocked Shift','voidshift'),('Force Clock Out','forceclockout'),('Recent Shifts','shiftlog'),('Shift Audit CSV','shiftaudit'),('Time Clock Post','setupclock')],
          'revenue': [('Performance','performance'),('Revenue CSV','performancecsv'),('Stream Segment CSV','streamreport'),('Post Leaderboard','postperformance'),('Commission Review Channel','reviewchannel'),('Void Approved Commission','voidcommission'),('Review Pending Commissions','pendingcommissions'),('Review Commission by ID','reviewcommission')],
          'pay': [('Weekly Payroll / Approve / Paid','weeklypayroll'),('Finalization Checklist','paychecklist'),('Review Payroll','payrollreview'),('Daily Pay Breakdown','dailypay'),('Payments & Balances','payments'),('Payroll CSV','payroll'),('Award Bonus','bonus'),('View Bonuses','bonuses'),('Void Bonus','voidbonus')],
          'admin': [('Pay Settings','settings'),('Employee Logs Channel','logchannel'),('Performance Channel','perfchannel'),('Report Incident','fine'),('Review Incidents','fines'),('Dismiss Incident','dismissfine'),('Incident CSV','finescsv'),('Database Backup','backup'),('Auto Backup Status','backupstatus'),('Manager Audit CSV','manageraudit'),('Business Expenses','businessexpenses'),('Full Manager Tools','advanced')]
        }
        select=discord.ui.Select(placeholder='Choose an action…',min_values=1,max_values=1,
                                 options=[discord.SelectOption(label=label,value=key) for label,key in groups[section]])
        async def action(interaction):
            if interaction.user.id!=self.owner_id or not manager(interaction) or not allowed_guild(interaction):
                await interaction.response.send_message('Manager access required.',ephemeral=True);return
            choice=select.values[0]
            await self.do_action(interaction,choice)
        select.callback=action
        self.add_item(select)

    async def do_action(self,interaction,choice):
        if choice=='manualweekly':
            return await interaction.response.send_message('**Weekly hours tracked externally**\nUse `/addmanualhours` to enter *missing* decimal hours for an unapproved Sunday–Saturday week. Use `/manualhours` to review and `/voidmanualhours` to correct a mistaken entry. These are NOT fake clock-in/out shifts. Weekly Payroll and Week CSV include them.',ephemeral=True)
        if choice=='businessexpenses':
            return await interaction.response.send_message('**Business Expenses**\nEmployees use `/submitexpense`. Managers use `/setexpensereviewchannel`, `/pendingexpenses`, `/reviewexpense ID`, `/expenses` for approved totals, `/expenseview ID` for evidence, `/expensescsv` for export, and `/voidexpense` for corrections.',ephemeral=True)
        if choice=='fixshift':
            rows=DB.execute('SELECT * FROM shifts WHERE guild_id=? ORDER BY id DESC LIMIT 25',(interaction.guild_id,)).fetchall()
            if not rows: return await interaction.response.send_message('No shifts to correct yet.',ephemeral=True)
            return await interaction.response.send_message('Select a shift to correct:',view=BossShiftPicker(self.owner_id,rows),ephemeral=True)
        if choice in ('addshift','bonus','fine'):
            label={'addshift':'missing shift','bonus':'bonus','fine':'incident'}[choice]
            return await interaction.response.send_message(f'Select an employee for the {label}:',view=BossMemberPicker(choice,self.owner_id),ephemeral=True)
        if choice in ('voidshift','forceclockout'):
            if choice=='voidshift':
                rows=DB.execute('SELECT * FROM shifts WHERE guild_id=? AND clock_out IS NOT NULL ORDER BY id DESC LIMIT 25',(interaction.guild_id,)).fetchall()
                if not rows:return await interaction.response.send_message('No closed shifts available to void.',ephemeral=True)
                return await interaction.response.send_message('Choose a shift to void (reason required):',view=V38ShiftPicker(self.owner_id,rows),ephemeral=True)
            rows=DB.execute('SELECT * FROM shifts WHERE guild_id=? AND clock_out IS NULL ORDER BY id DESC LIMIT 25',(interaction.guild_id,)).fetchall()
            if not rows:return await interaction.response.send_message('No active paid shifts. Employees on meal breaks are not clocked in.',ephemeral=True)
            return await interaction.response.send_message('Choose employee to force clock out:',view=V38ForcePicker(self.owner_id,rows),ephemeral=True)
        if choice=='pendingcommissions':
            return await interaction.response.send_message(pending_review_summary(interaction.guild_id),
                view=V41PendingReviewList(self.owner_id, interaction.guild_id), ephemeral=True)
        if choice=='reviewcommission':
            return await interaction.response.send_modal(V41CommissionIdModal())
        if choice=='voidcommission':
            rows=DB.execute("SELECT * FROM commissions WHERE guild_id=? AND status='approved' ORDER BY id DESC LIMIT 25",(interaction.guild_id,)).fetchall()
            if not rows: return await interaction.response.send_message('No approved commissions found.',ephemeral=True)
            return await interaction.response.send_message('Choose the incorrect approval (unfinalized payroll only):',view=CommissionVoidPicker(self.owner_id,rows),ephemeral=True)
        if choice in ('voidbonus','dismissfine'):
            isbonus=choice=='voidbonus'
            rows=(DB.execute('SELECT * FROM bonuses WHERE guild_id=? AND voided_at IS NULL ORDER BY id DESC LIMIT 25',(interaction.guild_id,)).fetchall()
                  if isbonus else DB.execute("SELECT * FROM incident_fines WHERE guild_id=? AND status!='dismissed' ORDER BY id DESC LIMIT 25",(interaction.guild_id,)).fetchall())
            if not rows:return await interaction.response.send_message('No matching records.',ephemeral=True)
            return await interaction.response.send_message('Choose a record:',view=BossRecordPicker('voidbonus' if isbonus else 'reviewfine',self.owner_id,rows),ephemeral=True)
        if choice in ('logchannel','reviewchannel','perfchannel'):
            key={'logchannel':'log_channel_id','reviewchannel':'review_channel_id','perfchannel':'performance_channel_id'}[choice]
            return await interaction.response.send_message('Select the private channel:',view=BossChannelPicker(self.owner_id,key),ephemeral=True)
        if choice=='advanced':
            return await interaction.response.send_message(embed=boss_embed(interaction.guild_id),view=BossDashboard(self.owner_id),ephemeral=True)
        if choice=='settings':
            return await interaction.response.send_message(embed=configuration_embed(interaction.guild_id),view=SettingsView(),ephemeral=True)
        standard={'hours':('hours',dict(days=7,employee=None)),
                  'shiftlog':('shiftlog',dict(employee=None,limit=10)),
                  'shiftaudit':('shiftaudit',{}),
                  'setupclock':('setupclock',{}),
                  'performance':('performance',dict(days=7,employee=None)),
                  'performancecsv':('performancecsv',dict(days=7)),
                  'streamreport':('streamreport',dict(days=7)),
                  'postperformance':('postperformance',dict(days=7)),
                  'payrollreview':('payrollreview',{}),
                  'dailypay':('dailypay',{}),
                  'payments':('payments',{}),
                  'weeklypayroll':('weeklypayroll',{}),
                  'paychecklist':('paychecklist',{}),
                  'payroll':('payroll',dict(days=7)),
                  'bonuses':('bonuses',dict(days=30,employee=None)),
                  'fines':('fines',dict(employee=None,days=30)),
                  'finescsv':('finescsv',dict(days=30)),
                  'backup':('backup',{}),
                  'backupstatus':('backupstatus',{}),
                  'manageraudit':('manageraudit',{})}
        if choice not in standard:
            return await interaction.response.send_message('Action not configured.',ephemeral=True)
        command,kwargs=standard[choice]
        await run_boss_command(interaction,command,**kwargs)




# V46: Business expense ledger (independent of wages, payroll, bonuses and fines).
# Stored under DATA_DIR/proofs, so existing full backups include receipt and photo evidence.
EXPENSE_PROOF_DIR = UPLOAD_DIR / 'business_expenses'
EXPENSE_PROOF_DIR.mkdir(parents=True, exist_ok=True)
DB.executescript("""
CREATE TABLE IF NOT EXISTS business_expenses (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 guild_id INTEGER NOT NULL,
 amount_cents INTEGER NOT NULL CHECK(amount_cents > 0),
 merchant TEXT NOT NULL,
 location TEXT NOT NULL,
 category TEXT NOT NULL,
 purchased_on TEXT NOT NULL,
 payment_method TEXT NOT NULL,
 notes TEXT NOT NULL DEFAULT '',
 receipt_path TEXT NOT NULL,
 photo_path TEXT NOT NULL,
 recorded_by INTEGER NOT NULL,
 recorded_at TEXT NOT NULL,
 voided_at TEXT,
 voided_by INTEGER,
 void_reason TEXT
);
CREATE INDEX IF NOT EXISTS business_expenses_date_idx
 ON business_expenses(guild_id,purchased_on,id);
""")
DB.commit()

EXPENSE_CATEGORIES = ['Inventory','Shipping','Supplies','Team meals','Equipment','Travel','Software','Other']


# V49: Friendly, attachment-specific validation for employee expense uploads.
EXPENSE_MAX_FILE_BYTES = 8 * 1024 * 1024
EXPENSE_MIME_SUFFIX = {'image/png': '.png', 'image/jpeg': '.jpg',
                       'image/webp': '.webp', 'application/pdf': '.pdf'}


def expense_detect_file_type(data):
    """Identify supported signatures plus common iPhone HEIC/HEIF images."""
    if not data:
        return 'empty'
    if data.startswith(b'%PDF-'):
        return 'pdf'
    if data.startswith(b'\x89PNG\r\n\x1a\n'):
        return 'png'
    if data.startswith(b'\xff\xd8\xff'):
        return 'jpg'
    if len(data) >= 12 and data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        return 'webp'
    if len(data) >= 12 and data[4:8] == b'ftyp':
        brand = data[8:12]
        if brand in (b'heic', b'heix', b'hevc', b'hevx', b'heim', b'heis',
                     b'hevm', b'hevs', b'heif', b'mif1', b'msf1'):
            return 'heic'
    return 'unknown'


def expense_attachment_suffix(attachment, label='Attachment', photo_only=False):
    mime = (attachment.content_type or '').split(';', 1)[0].strip().lower()
    suffix = EXPENSE_MIME_SUFFIX.get(mime)
    if not 0 < attachment.size <= EXPENSE_MAX_FILE_BYTES:
        raise ValueError(f'{label}: file must be nonempty and 8 MB or smaller.')
    if mime in ('image/heic', 'image/heif') or (attachment.filename or '').lower().endswith(('.heic', '.heif')):
        raise ValueError(f'{label}: HEIC/HEIF photos are not supported. On iPhone, save/export as JPEG or PNG (not just rename the file), then upload again.')
    if not suffix:
        raise ValueError(f'{label}: unsupported file type ({mime or "unknown"}). Upload a real PNG, JPG, WebP'+(' image.' if photo_only else ' image or PDF.')+' On iPhone, convert HEIC/HEIF to JPEG or PNG first.')
    if photo_only and suffix == '.pdf':
        raise ValueError(f'{label}: must be an image (PNG, JPG or WebP), not a PDF.')
    return suffix


def expense_file_bytes_ok(data, suffix):
    actual = expense_detect_file_type(data)
    return actual == {'.pdf':'pdf','.png':'png','.jpg':'jpg','.webp':'webp'}.get(suffix)


def expense_validate_uploaded_bytes(data, suffix, label='Attachment'):
    if len(data) > EXPENSE_MAX_FILE_BYTES:
        raise ValueError(f'{label}: file exceeds 8 MB.')
    actual = expense_detect_file_type(data)
    if actual == 'heic':
        raise ValueError(f'{label}: this is an iPhone HEIC/HEIF image, even if Discord labels it JPG or PNG. Convert/export it to a real JPEG or PNG before retrying; renaming the extension does not convert it.')
    if actual == 'empty':
        raise ValueError(f'{label}: uploaded file is empty. Please attach it again.')
    if actual == 'unknown':
        raise ValueError(f'{label}: could not recognize its file contents. Please export it as a real JPG, PNG, WebP'+(' or PDF.' if suffix == '.pdf' else '.') )
    if not expense_file_bytes_ok(data, suffix):
        raise ValueError(f'{label}: Discord labels this as {suffix.lstrip(".").upper()}, but the file is actually {actual.upper()}. Export it to a genuine JPG or PNG and retry (do not just rename it).')

def expense_evidence_path(raw):
    candidate=Path(raw).resolve()
    if not candidate.is_relative_to(EXPENSE_PROOF_DIR.resolve()) or not candidate.is_file():
        raise ValueError('Expense evidence file is unavailable.')
    return candidate


# V47: independent employee business-expense review pipeline.
# Approval transfers an expense to the existing manager ledger only once.
DB.executescript("""
CREATE TABLE IF NOT EXISTS expense_submissions (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 guild_id INTEGER NOT NULL,
 employee_id INTEGER NOT NULL,
 amount_cents INTEGER NOT NULL CHECK(amount_cents > 0),
 merchant TEXT NOT NULL,
 location TEXT NOT NULL,
 category TEXT NOT NULL,
 purchased_on TEXT NOT NULL,
 payment_method TEXT NOT NULL,
 notes TEXT NOT NULL,
 receipt_path TEXT NOT NULL,
 photo_path TEXT NOT NULL,
 submitted_at TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','approved','rejected')),
 reviewed_by INTEGER, reviewed_at TEXT, review_message_id INTEGER,
 ledger_expense_id INTEGER UNIQUE
);
CREATE INDEX IF NOT EXISTS expense_submissions_pending
 ON expense_submissions(guild_id,status,id);
CREATE TABLE IF NOT EXISTS expense_review_settings (
 guild_id INTEGER PRIMARY KEY, review_channel_id INTEGER NOT NULL
);
""")
DB.commit()


def expense_review_embed(row):
    embed = discord.Embed(
        title=f'🧾 Business expense submission #{row["id"]}',
        description=f'**Status:** {row["status"].upper()}\n**Submitted by:** <@{row["employee_id"]}>',
        color=discord.Color.orange() if row['status']=='pending' else
              (discord.Color.green() if row['status']=='approved' else discord.Color.red()))
    for key,value in [('Amount',money(row['amount_cents'])),('Merchant',row['merchant']),
                      ('Category',row['category']),('Location',row['location']),
                      ('Purchase date',row['purchased_on']),('Payment method',row['payment_method']),
                      ('Notes',row['notes'] or '—')]:
        embed.add_field(name=key,value=discord.utils.escape_markdown(str(value))[:1024] or '—',inline=False)
    if row['ledger_expense_id']:
        embed.add_field(name='Ledger entry',value=f'#{row["ledger_expense_id"]}')
    embed.set_footer(text='Employee business expenses • Not employee payroll or commissions')
    return embed


class ExpenseReview(discord.ui.View):
    def __init__(self, submission_id):
        super().__init__(timeout=None)
        self.submission_id=submission_id
        for child in self.children:
            child.custom_id=f'etbeast_demo:expense_review:{submission_id}:{child.label.lower()}'

    async def decide(self,interaction,decision):
        if not allowed_guild(interaction) or not manager(interaction):
            return await interaction.response.send_message('Manager access required.',ephemeral=True)
        await interaction.response.defer(ephemeral=True,thinking=True)
        try:
            async with lock:
                row=DB.execute('SELECT * FROM expense_submissions WHERE id=? AND guild_id=?',
                               (self.submission_id,interaction.guild_id)).fetchone()
                if not row:
                    return await interaction.followup.send('Expense submission not found.',ephemeral=True)
                if row['employee_id']==interaction.user.id:
                    return await interaction.followup.send('You cannot approve or reject your own expense.',ephemeral=True)
                if row['status']!='pending':
                    return await interaction.followup.send(f'Expense #{self.submission_id} is already {row["status"]}.',ephemeral=True)
                if decision=='approved':
                    # Verify proof exists before ledger insertion; failure leaves pending status.
                    expense_evidence_path(row['receipt_path'])
                    if row['photo_path']:
                        expense_evidence_path(row['photo_path'])
                with DB:
                    ledger_id=None
                    if decision=='approved':
                        cur=DB.execute('''INSERT INTO business_expenses
                          (guild_id,amount_cents,merchant,location,category,purchased_on,payment_method,
                           notes,receipt_path,photo_path,recorded_by,recorded_at)
                          VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                          (row['guild_id'],row['amount_cents'],row['merchant'],row['location'],row['category'],
                           row['purchased_on'],row['payment_method'],row['notes'],row['receipt_path'],
                           row['photo_path'],row['employee_id'],row['submitted_at']))
                        ledger_id=cur.lastrowid
                    DB.execute('''UPDATE expense_submissions SET status=?,reviewed_by=?,reviewed_at=?,ledger_expense_id=?
                                  WHERE id=? AND status='pending' ''',
                               (decision,interaction.user.id,now_iso(),ledger_id,self.submission_id))
                updated=DB.execute('SELECT * FROM expense_submissions WHERE id=?',(self.submission_id,)).fetchone()
        except (sqlite3.Error,ValueError,OSError) as exc:
            return await interaction.followup.send(f'Could not review expense: {exc}',ephemeral=True)
        await interaction.followup.send(
            f'✅ Expense #{self.submission_id} {decision}.'+
            (f' Recorded as business ledger expense #{ledger_id}.' if ledger_id else ' Not added to business expense totals.'),
            ephemeral=True)
        try:
            if interaction.message:
                await interaction.message.edit(embed=expense_review_embed(updated),view=None)
        except discord.HTTPException:
            pass
        try:
            person=interaction.guild.get_member(row['employee_id'])
            if person:
                await person.send(f'Your ETBeastDemo business expense #{self.submission_id} was {decision}.')
        except discord.HTTPException:
            pass

    @discord.ui.button(label='Approve',style=discord.ButtonStyle.success)
    async def approve(self,interaction,button):
        await self.decide(interaction,'approved')

    @discord.ui.button(label='Reject',style=discord.ButtonStyle.danger)
    async def reject(self,interaction,button):
        await self.decide(interaction,'rejected')


@bot.tree.command(name='setexpensereviewchannel',description='Manager: select private expense approval channel')
@app_commands.guild_only()
async def setexpensereviewchannel(interaction:discord.Interaction,channel:discord.TextChannel):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.',ephemeral=True)
    async with lock:
        with DB:
            DB.execute('''INSERT INTO expense_review_settings(guild_id,review_channel_id) VALUES (?,?)
               ON CONFLICT(guild_id) DO UPDATE SET review_channel_id=excluded.review_channel_id''',
               (interaction.guild_id,channel.id))
    await interaction.response.send_message(f'✅ Business expense reviews will go to {channel.mention}. '
       'Make this channel private to managers.',ephemeral=True)


@bot.tree.command(name='submitexpense',description='Employee: submit a business purchase for manager review')
@app_commands.describe(amount='Amount paid in USD',merchant='Merchant/vendor',location='Store location or website',
 category='Expense category',purchased_on='Purchase date YYYY-MM-DD',receipt='Required receipt photo or PDF',
 photo_proof='Optional additional photo of purchased item or proof',payment_method='Business card, cash, etc.',
 notes='Optional business purpose')
@app_commands.choices(category=[app_commands.Choice(name=c,value=c) for c in EXPENSE_CATEGORIES])
@app_commands.guild_only()
async def submitexpense(interaction:discord.Interaction,amount:str,merchant:str,location:str,
                        category:str,purchased_on:str,receipt:discord.Attachment,photo_proof:discord.Attachment=None,
                        payment_method:str='Business card',notes:str=''):
    if not allowed_guild(interaction) or not employee(interaction):
        return await interaction.response.send_message('Employee access required.',ephemeral=True)
    setting=DB.execute('SELECT review_channel_id FROM expense_review_settings WHERE guild_id=?',
                       (interaction.guild_id,)).fetchone()
    if not setting:
        return await interaction.response.send_message('Manager must configure /setexpensereviewchannel first.',ephemeral=True)
    review=interaction.guild.get_channel(setting['review_channel_id'])
    if not isinstance(review,discord.TextChannel):
        return await interaction.response.send_message('Expense review channel missing. Contact a manager.',ephemeral=True)
    try:
        dollars=Decimal(amount.strip().replace(',',''))
        if not dollars.is_finite() or dollars<=0 or dollars>Decimal('1000000'):
            raise ValueError('Amount must be greater than zero and at most $1,000,000.')
        cents=int((dollars*100).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
        if cents<=0: raise ValueError('Minimum amount is $0.01.')
        day=date.fromisoformat(purchased_on)
        if day>datetime.now(PAYROLL_TZ).date():raise ValueError('Purchase date cannot be in the future.')
        if category not in EXPENSE_CATEGORIES:raise ValueError('Invalid category.')
        for value,label,max_length in [(merchant,'Merchant',120),(location,'Location',200),
                                       (payment_method,'Payment method',80)]:
            if not value.strip() or len(value)>max_length:raise ValueError(f'{label} is required and must be under {max_length} characters.')
        if len(notes)>1000:raise ValueError('Notes must be at most 1,000 characters.')
        a=expense_attachment_suffix(receipt, 'Receipt')
        b=expense_attachment_suffix(photo_proof, 'Optional photo proof', photo_only=True) if photo_proof is not None else None
    except ValueError as exc:
        return await interaction.response.send_message(f'❌ {exc}',ephemeral=True)
    await interaction.response.defer(ephemeral=True,thinking=True)
    paths=[]
    try:
        data1=await receipt.read()
        data2=await photo_proof.read() if photo_proof is not None else None
        expense_validate_uploaded_bytes(data1, a, 'Receipt')
        if data2 is not None:
            expense_validate_uploaded_bytes(data2, b, 'Optional photo proof')
        from uuid import uuid4
        tag=uuid4().hex
        paths=[EXPENSE_PROOF_DIR/f'{tag}_receipt{a}']
        if data2 is not None:
            paths.append(EXPENSE_PROOF_DIR/f'{tag}_photo{b}')
        async with lock:
            paths[0].write_bytes(data1)
            if data2 is not None:
                paths[1].write_bytes(data2)
            with DB:
                cur=DB.execute('''INSERT INTO expense_submissions
                 (guild_id,employee_id,amount_cents,merchant,location,category,purchased_on,payment_method,
                 notes,receipt_path,photo_path,submitted_at)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                 (interaction.guild_id,interaction.user.id,cents,merchant.strip(),location.strip(),category,
                  day.isoformat(),payment_method.strip(),notes.strip(),str(paths[0]),str(paths[1]) if len(paths)>1 else '',now_iso()))
                eid=cur.lastrowid
        row=DB.execute('SELECT * FROM expense_submissions WHERE id=?',(eid,)).fetchone()
        try:
            msg=await review.send(embed=expense_review_embed(row),
                                  files=[discord.File(path) for path in paths],
                                  view=ExpenseReview(eid),allowed_mentions=discord.AllowedMentions.none())
            async with lock:
                with DB:
                    DB.execute('UPDATE expense_submissions SET review_message_id=? WHERE id=?',(msg.id,eid))
        except discord.HTTPException:
            # Submission persists and can be processed with /reviewexpense even if Discord posting fails.
            await interaction.followup.send(f'⚠️ Expense #{eid} saved as pending, but the review channel post failed. '
              'Ask a manager to run /reviewexpense with this ID.',ephemeral=True)
            return
        await interaction.followup.send(f'✅ Expense #{eid} submitted for approval. Not yet added to business expenses.',ephemeral=True)
    except (discord.HTTPException,OSError,sqlite3.Error,ValueError) as exc:
        await interaction.followup.send(f'❌ Expense submission failed: {exc}',ephemeral=True)


@bot.tree.command(name='reviewexpense',description='Manager: review a pending business expense by ID')
@app_commands.guild_only()
async def reviewexpense(interaction:discord.Interaction,expense_id:int):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.',ephemeral=True)
    row=DB.execute('SELECT * FROM expense_submissions WHERE id=? AND guild_id=?',
                   (expense_id,interaction.guild_id)).fetchone()
    if not row:return await interaction.response.send_message('Expense submission not found.',ephemeral=True)
    if row['status']!='pending':
        return await interaction.response.send_message(embed=expense_review_embed(row),ephemeral=True)
    await interaction.response.send_message(embed=expense_review_embed(row),view=ExpenseReview(expense_id),ephemeral=True)


@bot.tree.command(name='pendingexpenses',description='Manager: list expense submissions awaiting review')
@app_commands.guild_only()
async def pendingexpenses(interaction:discord.Interaction):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.',ephemeral=True)
    rows=DB.execute('''SELECT * FROM expense_submissions WHERE guild_id=? AND status='pending'
                       ORDER BY submitted_at ASC LIMIT 20''',(interaction.guild_id,)).fetchall()
    total=DB.execute("SELECT COUNT(*) FROM expense_submissions WHERE guild_id=? AND status='pending'",
                     (interaction.guild_id,)).fetchone()[0]
    if not rows:return await interaction.response.send_message('✅ No pending business expenses.',ephemeral=True)
    lines=[f'**🧾 Pending business expenses: {total}**']
    for r in rows:
        lines.append(f'#{r["id"]} · {money(r["amount_cents"])} · {discord.utils.escape_markdown(r["merchant"][:60])} '
                     f'· <@{r["employee_id"]}> — `/reviewexpense {r["id"]}`')
    await interaction.response.send_message('\n'.join(lines),ephemeral=True,
                                            allowed_mentions=discord.AllowedMentions.none())

@bot.tree.command(name='addexpense',description='Manager: record business expense with receipt and separate photo proof')
@app_commands.describe(amount='Amount in USD, e.g. 45.75',merchant='Store or vendor',
 location='Where bought (store address, city, or website)',category='Expense category',
 purchased_on='Purchase date YYYY-MM-DD',receipt='Receipt image or PDF (required)',
 photo_proof='Optional photo of purchased item or payment',
 payment_method='Business Gold, cash, bank transfer, etc.',notes='Optional details')
@app_commands.choices(category=[app_commands.Choice(name=c,value=c) for c in EXPENSE_CATEGORIES])
@app_commands.guild_only()
async def addexpense(interaction:discord.Interaction,amount:str,merchant:str,location:str,
                     category:str,purchased_on:str,receipt:discord.Attachment,
                     photo_proof:discord.Attachment=None,payment_method:str='Business card',notes:str=''):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.',ephemeral=True)
    try:
        usd=Decimal(amount.strip().replace(',',''))
        if not usd.is_finite() or usd<=0 or usd>Decimal('1000000'):
            raise ValueError('Amount must be greater than $0 and at most $1,000,000.')
        cents=int((usd*100).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
        if cents<=0: raise ValueError('Amount must be at least $0.01.')
        day=date.fromisoformat(purchased_on)
        if day>datetime.now(ZoneInfo('America/New_York')).date():
            raise ValueError('Purchase date cannot be in the future.')
        if category not in EXPENSE_CATEGORIES: raise ValueError('Invalid category.')
        for value,label,maxlen in [(merchant,'Merchant',120),(location,'Location',200),
                                   (payment_method,'Payment method',80)]:
            if not value.strip() or len(value.strip())>maxlen: raise ValueError(f'{label} is required (maximum {maxlen} characters).')
        if len(notes)>1000: raise ValueError('Notes must be 1,000 characters or fewer.')
        receipt_ext=expense_attachment_suffix(receipt)
        photo_ext=expense_attachment_suffix(photo_proof) if photo_proof is not None else None
        if photo_ext=='.pdf':raise ValueError('Photo proof must be an image, not a PDF.')
    except ValueError as exc:
        return await interaction.response.send_message(f'❌ {exc}',ephemeral=True)
    await interaction.response.defer(ephemeral=True,thinking=True)
    try:
        receipt_data=await receipt.read()
        photo_data=await photo_proof.read() if photo_proof is not None else None
        if not expense_file_bytes_ok(receipt_data,receipt_ext) or (photo_data is not None and not expense_file_bytes_ok(photo_data,photo_ext)):
            raise ValueError('Receipt or photo content does not match its file type.')
        if len(receipt_data)>8*1024*1024 or (photo_data is not None and len(photo_data)>8*1024*1024):
            raise ValueError('Each attachment must be 8 MB or smaller.')
        async with lock:
            # Upload files with unique names, then commit record. If writing fails, remove files.
            from uuid import uuid4
            uid=uuid4().hex
            receipt_path=EXPENSE_PROOF_DIR/f'{uid}_receipt{receipt_ext}'
            photo_path=EXPENSE_PROOF_DIR/f'{uid}_photo{photo_ext}' if photo_data is not None else None
            try:
                receipt_path.write_bytes(receipt_data)
                if photo_path is not None:
                    photo_path.write_bytes(photo_data)
                with DB:
                    cur=DB.execute("""INSERT INTO business_expenses
                      (guild_id,amount_cents,merchant,location,category,purchased_on,payment_method,
                       notes,receipt_path,photo_path,recorded_by,recorded_at)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (interaction.guild_id,cents,merchant.strip(),location.strip(),category,
                       day.isoformat(),payment_method.strip(),notes.strip(),str(receipt_path),
                       str(photo_path) if photo_path is not None else '',interaction.user.id,now_iso()))
                expense_id=cur.lastrowid
            except Exception:
                receipt_path.unlink(missing_ok=True)
                if photo_path is not None:
                    photo_path.unlink(missing_ok=True)
                raise
        await interaction.followup.send(
            f'✅ Business expense **#{expense_id}** recorded: **{money(cents)}** at '
            f'**{discord.utils.escape_markdown(merchant.strip())}** ({category}) on {day.isoformat()}. '
            'Receipt saved privately; additional photo saved if supplied. Not included in payroll.',ephemeral=True)
    except (discord.HTTPException,OSError,sqlite3.Error,ValueError) as exc:
        await interaction.followup.send(f'❌ Could not save expense: {exc}',ephemeral=True)


@bot.tree.command(name='expenses',description='Manager: see business expense totals and recent entries')
@app_commands.describe(days='Lookback period 1–365 days (default 30)',category='Optional category filter')
@app_commands.choices(category=[app_commands.Choice(name=c,value=c) for c in EXPENSE_CATEGORIES])
@app_commands.guild_only()
async def expenses(interaction:discord.Interaction,days:app_commands.Range[int,1,365]=30,category:str=None):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.',ephemeral=True)
    cutoff=(datetime.now(ZoneInfo('America/New_York')).date()-timedelta(days=days-1)).isoformat()
    where='guild_id=? AND purchased_on>=? AND voided_at IS NULL'
    params=[interaction.guild_id,cutoff]
    if category: where+=' AND category=?';params.append(category)
    rows=DB.execute('SELECT * FROM business_expenses WHERE '+where+' ORDER BY purchased_on DESC,id DESC LIMIT 15',params).fetchall()
    totals=DB.execute('SELECT COUNT(*) count,COALESCE(SUM(amount_cents),0) cents FROM business_expenses WHERE '+where,params).fetchone()
    embed=discord.Embed(title='🧾 ETBeastDemo Business Expenses',color=discord.Color.blurple())
    embed.description=f'Last {days} days · {category or "All categories"}\n**Total: {money(totals["cents"])}** across {totals["count"]} expenses'
    if not rows:embed.add_field(name='Entries',value='No recorded expenses for this period.')
    for r in rows:
        embed.add_field(name=f'#{r["id"]} · {money(r["amount_cents"])} · {r["purchased_on"]}',
          value=f'{discord.utils.escape_markdown(r["merchant"][:80])} · {r["category"]}\n{discord.utils.escape_markdown(r["location"][:100])}',inline=False)
    embed.set_footer(text='Manager-only · Use /expenseview ID for receipt and photo · /expensescsv to export')
    await interaction.response.send_message(embed=embed,ephemeral=True)


@bot.tree.command(name='expenseview',description='Manager: review an expense and its two evidence files')
@app_commands.guild_only()
async def expenseview(interaction:discord.Interaction,expense_id:int):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.',ephemeral=True)
    row=DB.execute('SELECT * FROM business_expenses WHERE guild_id=? AND id=?',(interaction.guild_id,expense_id)).fetchone()
    if not row:return await interaction.response.send_message('Expense not found.',ephemeral=True)
    await interaction.response.defer(ephemeral=True,thinking=True)
    embed=discord.Embed(title=f'🧾 Business Expense #{row["id"]}',color=discord.Color.orange() if row['voided_at'] else discord.Color.green())
    for name,value in [('Amount',money(row['amount_cents'])),('Merchant',row['merchant']),('Location',row['location']),
                       ('Category',row['category']),('Purchase date',row['purchased_on']),('Payment method',row['payment_method']),
                       ('Recorded by',f'<@{row["recorded_by"]}>'),('Notes',row['notes'] or '—'),
                       ('Status','Voided' if row['voided_at'] else 'Active')]:
        embed.add_field(name=name,value=discord.utils.escape_markdown(value)[:1024] if name not in ('Recorded by',) else value,inline=False)
    try:
        paths=[(expense_evidence_path(row['receipt_path']),'receipt')]
        if row['photo_path']:
            paths.append((expense_evidence_path(row['photo_path']),'photo'))
        files=[discord.File(path,filename=f'expense_{expense_id}_{kind}{path.suffix}') for path,kind in paths]
        await interaction.followup.send(embed=embed,files=files,ephemeral=True,allowed_mentions=discord.AllowedMentions.none())
    except (ValueError,discord.HTTPException,OSError) as exc:
        await interaction.followup.send(f'Evidence could not be attached: {exc}',embed=embed,ephemeral=True)


@bot.tree.command(name='voidexpense',description='Manager: void a mistaken expense without deleting its audit record')
@app_commands.guild_only()
async def voidexpense(interaction:discord.Interaction,expense_id:int,reason:str):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.',ephemeral=True)
    if len(reason.strip())<5 or len(reason)>300:
        return await interaction.response.send_message('Provide a reason (5–300 characters).',ephemeral=True)
    async with lock:
        with DB:
            result=DB.execute("""UPDATE business_expenses SET voided_at=?,voided_by=?,void_reason=?
                WHERE guild_id=? AND id=? AND voided_at IS NULL""",
                (now_iso(),interaction.user.id,reason.strip(),interaction.guild_id,expense_id))
    await interaction.response.send_message(
      f'✅ Expense #{expense_id} voided and excluded from totals; its receipt and audit record remain.' if result.rowcount else 'Expense not found or already voided.',ephemeral=True)


@bot.tree.command(name='expensescsv',description='Manager: export business expense ledger as CSV')
@app_commands.describe(days='Lookback period 1–3650 days (default 365)')
@app_commands.guild_only()
async def expensescsv(interaction:discord.Interaction,days:app_commands.Range[int,1,3650]=365):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.',ephemeral=True)
    cutoff=(datetime.now(ZoneInfo('America/New_York')).date()-timedelta(days=days-1)).isoformat()
    rows=DB.execute('SELECT * FROM business_expenses WHERE guild_id=? AND purchased_on>=? ORDER BY purchased_on,id',
                    (interaction.guild_id,cutoff)).fetchall()
    output=io.StringIO();writer=csv.writer(output)
    headers=['id','purchased_on','amount_usd','merchant','location','category','payment_method','notes',
             'recorded_by','recorded_at','status','voided_by','voided_at','void_reason','receipt_filename','photo_filename']
    writer.writerow(headers)
    for r in rows:
        values=[r['id'],r['purchased_on'],f'{r["amount_cents"]/100:.2f}',r['merchant'],r['location'],
                r['category'],r['payment_method'],r['notes'],r['recorded_by'],r['recorded_at'],
                'voided' if r['voided_at'] else 'active',r['voided_by'],r['voided_at'],r['void_reason'],
                Path(r['receipt_path']).name,Path(r['photo_path']).name if r['photo_path'] else '']
        # Guard against CSV formula injection in user-provided strings.
        writer.writerow(["'"+v if isinstance(v,str) and v.lstrip().startswith(('=','+','-','@')) else v for v in values])
    data=io.BytesIO(output.getvalue().encode('utf-8-sig'))
    await interaction.response.send_message(f'🧾 Exported {len(rows)} expense records (including voided entries).',
        file=discord.File(data,filename='etbeast_demo_business_expenses.csv'),ephemeral=True)


@bot.tree.command(name='employeedashboard', description='Open your private employee dashboard')
@app_commands.guild_only()
async def employeedashboard(interaction:discord.Interaction):
    if not allowed_guild(interaction) or not employee(interaction):
        return await interaction.response.send_message('Employee access required.',ephemeral=True)
    await interaction.response.send_message(embed=employee_home_embed(interaction),view=EmployeeHome(interaction.user.id),ephemeral=True)


@bot.tree.command(name='managerdashboard', description='Manager: open simplified management dashboard')
@app_commands.guild_only()
async def managerdashboard(interaction:discord.Interaction):
    if not allowed_guild(interaction) or not manager(interaction):
        return await interaction.response.send_message('Manager access required.',ephemeral=True)
    await interaction.response.send_message(embed=boss_embed(interaction.guild_id),view=ManagerHome(interaction.user.id),ephemeral=True)


if __name__ == '__main__':
    if not TOKEN:
        raise SystemExit('Set DISCORD_TOKEN in .env before starting.')
    bot.run(TOKEN)
