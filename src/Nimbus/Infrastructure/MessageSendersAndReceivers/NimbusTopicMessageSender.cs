using System;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Nimbus.Infrastructure.Logging;

namespace Nimbus.Infrastructure.MessageSendersAndReceivers
{
    internal class NimbusTopicMessageSender : INimbusMessageSender
    {
        private readonly IQueueManager _queueManager;
        private readonly string _topicPath;
        private readonly ILogger _logger;

        private TopicClient _topicClient;

        public NimbusTopicMessageSender(IQueueManager queueManager, string topicPath, ILogger logger)
        {
            _queueManager = queueManager;
            _topicPath = topicPath;
            _logger = logger;
        }

        public async Task Send(BrokeredMessage toSend)
        {
            var topicClient = GetTopicClient();

            try
            {
                _logger.LogDispatchAction("Calling SendAsync", _topicPath, toSend);
                await topicClient.SendAsync(toSend);
            }
            catch (Exception)
            {
                DiscardTopicClient();
                throw;
            }
        }

        private TopicClient GetTopicClient()
        {
            if (_topicClient != null) return _topicClient;

            _topicClient = _queueManager.CreateTopicSender(_topicPath).Result;
            return _topicClient;
        }

        private void DiscardTopicClient()
        {
            var topicClient = _topicClient;
            _topicClient = null;

            if (topicClient == null) return;
            if (topicClient.IsClosed) return;

            try
            {
                _logger.Info("Discarding message sender for {TopicPath}", _topicPath);
                topicClient.Close();
            }
            catch (Exception exc)
            {
                _logger.Error(exc, "Failed to close TopicClient instance before discarding it.");
            }
        }

        public void Dispose()
        {
            DiscardTopicClient();
        }
    }
}