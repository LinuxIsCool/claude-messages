import path from 'node:path';
import { projectGmailSchedule } from './gmail-schedule-projector.js';

const home = process.env.HOME ?? '';
const messagesDir = process.env.LEGION_MESSAGES_DATA_DIR ?? path.join(home, '.claude', 'local', 'messages');
const calendarDir = process.env.LEGION_CALENDAR_DATA_DIR ?? path.join(home, '.claude', 'local', 'calendar');

const receipt = projectGmailSchedule({
  messagesDbPath: process.env.LEGION_MESSAGES_DB ?? path.join(messagesDir, 'messages.db'),
  scheduleDbPath: process.env.LEGION_GMAIL_SCHEDULE_DB ?? path.join(calendarDir, 'gmail-schedule.db'),
  receiptPath: process.env.LEGION_GMAIL_SCHEDULE_RECEIPT ?? path.join(calendarDir, 'gmail-schedule-observation.json'),
  healthPath: process.env.LEGION_MESSAGES_HEALTH ?? path.join(messagesDir, 'health.json'),
});

console.log(JSON.stringify(receipt));
if (receipt.status !== 'ok') process.exitCode = 1;
