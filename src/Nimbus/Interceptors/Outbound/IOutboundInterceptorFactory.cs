﻿using Nimbus.DependencyResolution;

namespace Nimbus.Interceptors.Outbound
{
    internal interface IOutboundInterceptorFactory
    {
        IOutboundInterceptor[] CreateInterceptors(IDependencyResolverScope scope);
    }
}