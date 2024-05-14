#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef struct future CFuture;

typedef enum {
  CANCEL = 0,
  TIMEOUT = 1,
  DEADLINE_EXCEED = 2
} CFutureCancellation;

#ifdef __cplusplus
}
#endif