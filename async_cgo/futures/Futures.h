#pragma once

class FutureInterface
{
  virtual bool isReady() const = 0;

  virtual void cancel() = 0;
};