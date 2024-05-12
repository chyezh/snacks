#include "future_c.h"

extern "C" void future_set_callback(Future* future, Callback callback,
                                    void* callback_parameter) {
  unlockMutex(callback_parameter);
}

extern "C" int future_is_ready(Future* future) { return 1; }

extern "C" void future_cancel(Future* future) {}