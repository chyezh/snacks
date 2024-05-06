#pragma once

#include "future_c_types.h"

extern void unlockMutex(void*);

#ifdef __cplusplus
extern "C" {
#endif

void future_set_callback(Future* future, Callback callback,
                         void* callback_parameter);

int future_is_ready(Future* future);

void future_cancel(Future* future);

#ifdef __cplusplus
}
#endif
