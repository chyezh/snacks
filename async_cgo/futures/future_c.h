#pragma once

#include "future_c_types.h"

extern void unlockMutex(void*);

#ifdef __cplusplus
extern "C" {
#endif

void future_cancel(CFuture* future);

void future_deadline(CFuture* future);

void future_timeout(CFuture* future);

#ifdef __cplusplus
}
#endif
