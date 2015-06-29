using System;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Nimbus.Infrastructure.Logging;

namespace Nimbus.Infrastructure.MessageSendersAndReceivers
{
    internal class NimbusQueueMessageSender : INimbusMessageSender
    {
        private readonly IQueueManager _queueManager;
        private readonly string _queuePath;
        private readonly ILogger _logger;

        private MessageSender _messageSender;

        public NimbusQueueMessageSender(IQueueManager queueManager, string queuePath, ILogger logger)
        {
            _queueManager = queueManager;
            _queuePath = queuePath;
            _logger = logger;
        }

        public async Task Send(BrokeredMessage toSend)
        {
            var messageSender = GetMessageSender();

            _logger.LogDispatchAction("Calling SendAsync", _queuePath, toSend);
            try
            {
                await messageSender.SendAsync(toSend);
            }
            catch (Exception)
            {
                DiscardMessageSender();
                throw;
            }
        }

        private MessageSender GetMessageSender()
        {
            if (_messageSender != null) return _messageSender;

            _messageSender = _queueManager.CreateMessageSender(_queuePath).Result;
            return _messageSender;
        }

        private void DiscardMessageSender()
        {
            var messageSender = _messageSender;
            _messageSender = null;

            if (messageSender == null) return;
            if (messageSender.IsClosed) return;

            try
            {
                _logger.Info("Discarding message sender for {QueuePath}", _queuePath);
                messageSender.Close();
            }
            catch (Exception exc)
            {
                _logger.Error(exc, "Failed to close MessageSender instance before discarding it.");
            }
        }

        public void Dispose()
        {
            DiscardMessageSender();
        }
    }
}