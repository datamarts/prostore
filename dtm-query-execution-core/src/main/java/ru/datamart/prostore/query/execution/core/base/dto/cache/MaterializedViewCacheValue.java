/*
 * Copyright © 2022 DATAMART LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.datamart.prostore.query.execution.core.base.dto.cache;

import ru.datamart.prostore.common.model.ddl.Entity;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class MaterializedViewCacheValue {
    private final Entity entity;
    private final AtomicReference<UUID> uuid = new AtomicReference<>(UUID.randomUUID());
    private final AtomicReference<MaterializedViewSyncStatus> status = new AtomicReference<>(MaterializedViewSyncStatus.READY);
    private final AtomicLong failsCount = new AtomicLong();
    private final AtomicReference<LocalDateTime> lastSyncTime = new AtomicReference<>();
    private final AtomicReference<Throwable> lastSyncError = new AtomicReference<>();
    private final AtomicBoolean inSync = new AtomicBoolean();

    public MaterializedViewCacheValue(Entity entity) {
        this.entity = entity;
    }

    public Entity getEntity() {
        return entity;
    }

    public UUID getUuid() {
        return uuid.get();
    }

    public void markForDeletion() {
        this.uuid.set(null);
    }

    public boolean isMarkedForDeletion() {
        return uuid.get() == null;
    }

    public MaterializedViewSyncStatus getStatus() {
        return status.get();
    }

    public void setStatus(MaterializedViewSyncStatus value) {
        status.set(value);
    }

    public long getFailsCount() {
        return failsCount.get();
    }

    public void resetFailsCount() {
        failsCount.set(0);
    }

    public void incrementFailsCount() {
        failsCount.incrementAndGet();
    }

    public LocalDateTime getLastSyncTime() {
        return lastSyncTime.get();
    }

    public void setLastSyncTime(LocalDateTime value) {
        lastSyncTime.set(value);
    }

    public Throwable getLastSyncError() {
        return lastSyncError.get();
    }

    public void setLastSyncError(Throwable value) {
        lastSyncError.set(value);
    }

    public void resetLastSyncError() {
        lastSyncError.set(null);
    }

    public boolean isInSync() {
        return inSync.get();
    }

    public void setInSync() {
        inSync.set(true);
    }

    public void setNotInSync() {
        inSync.set(false);
    }
}
