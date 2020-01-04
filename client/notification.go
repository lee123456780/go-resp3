/*
Copyright 2019 Stefan Miller

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

// GenericNotification represents the type for an out of bound push notification send by Redis.
type genericNotification struct {
	kind   string
	values Slice
}

// SubscribeNotification represents the type for an out of bound subscribe push notification send by Redis.
type subscribeNotification struct {
	channel string // channel or pattern
	count   int64
}

// UnsubscribeNotification represents the type for an out of bound unsubscribe push notification send by Redis.
type unsubscribeNotification struct {
	channel string
	count   int64
}

// PublishNotification represents the type for an out of bound publish push notification send by Redis.
type publishNotification struct {
	pattern string
	channel string
	msg     string
}

// InvalidateNotification represents the type for an out of bound invalidation push notification send by Redis (client side caching).
type invalidateNotification uint32
