const DEFAULT_TIME_ZONE = "Asia/Singapore";
const DEFAULT_START_HOUR = 9;
const DEFAULT_END_HOUR = 21;

function integerEnv(name: string, fallback: number) {
  const parsed = Number(process.env[name]);
  return Number.isInteger(parsed) ? parsed : fallback;
}

export function configuredServiceWindow() {
  return {
    timeZone: process.env.SERVICE_WINDOW_TIME_ZONE ?? DEFAULT_TIME_ZONE,
    startHour: integerEnv("SERVICE_WINDOW_START_HOUR", DEFAULT_START_HOUR),
    endHour: integerEnv("SERVICE_WINDOW_END_HOUR", DEFAULT_END_HOUR),
  };
}

export function isWithinServiceWindow(date = new Date()) {
  const { timeZone, startHour, endHour } = configuredServiceWindow();
  const hourPart = new Intl.DateTimeFormat("en-SG", {
    hour: "2-digit",
    hourCycle: "h23",
    timeZone,
  }).formatToParts(date).find((part) => part.type === "hour")?.value;
  const hour = Number(hourPart);

  if (!Number.isInteger(hour)) return false;
  if (startHour === endHour) return true;
  if (startHour < endHour) return hour >= startHour && hour < endHour;
  return hour >= startHour || hour < endHour;
}

export function serviceWindowMessage() {
  const { timeZone, startHour, endHour } = configuredServiceWindow();
  return `Databricks access is paused outside ${startHour}:00-${endHour}:00 ${timeZone} to stay within usage limits. Please try again during the daily service window.`;
}
