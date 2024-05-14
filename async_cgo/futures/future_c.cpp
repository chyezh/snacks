#include "future_c.h"

#include "Futures.h"

extern "C" void future_cancel(CFuture* future,
                              CFutureCancellation* cancellation) {
  switch (*cancellation) {
    case CANCEL:
      static_cast<IFuture*>(future)->cancel(folly::FutureCancellation());
      break;
    case TIMEOUT:
      static_cast<IFuture*>(future)->cancel(folly::FutureTimeout());
      break;
    case DEADLINE_EXCEED:
      static_cast<IFuture*>(future)->cancel(folly::FutureDeadlineExceed());
      break;
  }
}