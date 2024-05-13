#pragma once

#include "future_c_types.h"

extern void unlockMutex(void*);

#ifdef __cplusplus
extern "C" {
#endif

void future_run(CFuture* future);

void future_set_callback(CFuture* future, Callback callback,
                         void* callback_parameter);

int future_is_ready(CFuture* future);

void future_cancel(CFuture* future);

#ifdef __cplusplus
}
#endif
