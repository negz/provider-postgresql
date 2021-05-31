/*
Copyright 2020 The Crossplane Authors.

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

package extension

import (
	"context"
	"strings"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"

	"github.com/crossplane-contrib/provider-sql/apis/postgresql/v1alpha1"
	"github.com/crossplane-contrib/provider-sql/pkg/clients/postgresql"
	"github.com/crossplane-contrib/provider-sql/pkg/clients/xsql"
)

const (
	errTrackPCUsage = "cannot track ProviderConfig usage"
	errGetPC        = "cannot get ProviderConfig"
	errNoSecretRef  = "ProviderConfig does not reference a credentials Secret"
	errGetSecret    = "cannot get credentials Secret"

	errNotExtension    = "managed resource is not a Extension custom resource"
	errSelectExtension = "cannot select extension"
	errCreateExtension = "cannot create extension"
	errDropExtension   = "cannot drop extension"

	maxConcurrency = 5
)

// Setup adds a controller that reconciles Extension managed resources.
func Setup(mgr ctrl.Manager, l logging.Logger) error {
	name := managed.ControllerName(v1alpha1.ExtensionGroupKind)

	t := resource.NewProviderConfigUsageTracker(mgr.GetClient(), &v1alpha1.ProviderConfigUsage{})
	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.ExtensionGroupVersionKind),
		managed.WithExternalConnecter(&connector{kube: mgr.GetClient(), usage: t, newDB: postgresql.New}),
		managed.WithLogger(l.WithValues("controller", name)),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&v1alpha1.Extension{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrency,
		}).
		Complete(r)
}

type connector struct {
	kube  client.Client
	usage resource.Tracker
	newDB func(creds map[string][]byte, database string, sslmode string) xsql.DB
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha1.Extension)
	if !ok {
		return nil, errors.New(errNotExtension)
	}

	if err := c.usage.Track(ctx, mg); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	// ProviderConfigReference could theoretically be nil, but in practice the
	// DefaultProviderConfig initializer will set it before we get here.
	pc := &v1alpha1.ProviderConfig{}
	if err := c.kube.Get(ctx, types.NamespacedName{Name: cr.GetProviderConfigReference().Name}, pc); err != nil {
		return nil, errors.Wrap(err, errGetPC)
	}

	// We don't need to check the credentials source because we currently only
	// support one source (PostgreSQLConnectionSecret), which is required and
	// enforced by the ProviderConfig schema.
	ref := pc.Spec.Credentials.ConnectionSecretRef
	if ref == nil {
		return nil, errors.New(errNoSecretRef)
	}

	s := &corev1.Secret{}
	if err := c.kube.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, s); err != nil {
		return nil, errors.Wrap(err, errGetSecret)
	}

	// We do not want to create an extension on the default DB
	// if the user was expecting a database name to be resolved.
	if cr.Spec.ForProvider.Database != nil {
		return &external{db: c.newDB(s.Data, *cr.Spec.ForProvider.Database, pc.Spec.SslMode)}, nil
	}

	return &external{db: c.newDB(s.Data, "", pc.Spec.SslMode)}, nil
}

type external struct{ db xsql.DB }

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha1.Extension)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotExtension)
	}

	// If the Extension exists, it will have all of these properties.
	observed := v1alpha1.ExtensionParameters{
		Version: new(string),
	}

	query := "SELECT " +
		"extversion " +
		"FROM pg_extension " +
		"WHERE extname = $1"

	err := c.db.Scan(ctx, xsql.Query{
		String:     query,
		Parameters: []interface{}{cr.Spec.ForProvider.Extension},
	},
		observed.Version,
	)

	// If the database we try to connect on does not exist then
	// there cannot be an extension on that database either.
	if xsql.IsNoRows(err) || postgresql.IsInvalidCatalog(err) {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errSelectExtension)
	}

	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceLateInitialized: lateInit(observed, &cr.Spec.ForProvider),
		ResourceUpToDate:        upToDate(observed, cr.Spec.ForProvider),
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) { //nolint:gocyclo
	cr, ok := mg.(*v1alpha1.Extension)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotExtension)
	}

	var b strings.Builder
	b.WriteString("CREATE EXTENSION IF NOT EXISTS ")
	b.WriteString(pq.QuoteIdentifier(cr.Spec.ForProvider.Extension))

	if cr.Spec.ForProvider.Version != nil {
		b.WriteString(" WITH VERSION ")
		b.WriteString(pq.QuoteIdentifier(*cr.Spec.ForProvider.Version))
	}

	return managed.ExternalCreation{}, errors.Wrap(c.db.Exec(ctx, xsql.Query{String: b.String()}), errCreateExtension)
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) { //nolint:gocyclo
	_, ok := mg.(*v1alpha1.Extension)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotExtension)
	}

	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Extension)
	if !ok {
		return errors.New(errNotExtension)
	}

	err := c.db.Exec(ctx, xsql.Query{String: "DROP EXTENSION IF EXISTS " + pq.QuoteIdentifier(cr.Spec.ForProvider.Extension)})
	return errors.Wrap(err, errDropExtension)
}

func upToDate(observed, desired v1alpha1.ExtensionParameters) bool {
	if desired.Version == nil || (observed.Version != nil && *desired.Version == *observed.Version) {
		return true
	}
	return false
}

func lateInit(observed v1alpha1.ExtensionParameters, desired *v1alpha1.ExtensionParameters) bool {
	li := false

	if desired.Version == nil && observed.Version != nil {
		desired.Version = observed.Version
		li = true
	}

	return li
}
