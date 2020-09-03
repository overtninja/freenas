from datetime import timedelta

from middlewared.alert.base import AlertClass, AlertCategory, AlertLevel, Alert, ThreadedAlertSource
from middlewared.alert.schedule import IntervalSchedule


class VolumeVersionAlertClass(AlertClass):
    category = AlertCategory.STORAGE
    level = AlertLevel.WARNING
    title = "New Feature Flags Are Available for Pool"
    text = (
        "New ZFS version or feature flags are available for pool %s. Please see <a href=\""
        "https://www.truenas.com/docs/hub/tasks/advanced/upgrading-pool/\">"
        "Upgrading a ZFS Pool</a> for details."
    )


class VolumeVersionAlertSource(ThreadedAlertSource):
    schedule = IntervalSchedule(timedelta(minutes=5))

    def check_sync(self):
        alerts = []
        for pool in self.middleware.call_sync("pool.query"):
            if not self.middleware.call_sync('pool.is_upgraded', pool["id"]):
                alerts.append(Alert(
                    VolumeVersionAlertClass,
                    pool["name"],
                ))

        return alerts
